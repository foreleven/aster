import { randomUUID } from "node:crypto";
import { Effect, FileSystem, Path, Predicate, Redacted, Schema, Stream } from "effect";
import {
  ExternalAgentError,
  taskPrompt,
  type ExternalAgent,
  type ExecutionSession,
  type ExecutionStatus,
} from "@aster/core";
import { makeCodexRpc } from "./rpc.js";
import {
  controlResponse,
  decodeControl,
  Row,
  ThreadRead,
  ThreadStart,
  TurnStart,
} from "./protocol.js";

export const makeCodexExecutor = Effect.fn("Codex.executor")(function* (
  environment: Readonly<Record<string, string | undefined>>,
  taskRoot: string,
  executable: string = "codex",
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const rpc = yield* makeCodexRpc(environment, executable);
  const connectionId = yield* Effect.sync(randomUUID);
  const controls = new Map<
    string,
    {
      rpcId: string | number;
      method: string;
      params: typeof Row.Type;
      request: Awaited<Effect.Success<ReturnType<typeof decodeControl>>>;
    }
  >();
  const owned = new Set<string>();
  const sessions = new Map<string, ExecutionSession>();
  const error = (operation: ExternalAgentError["operation"]) => (cause: unknown) =>
    cause instanceof ExternalAgentError
      ? cause
      : new ExternalAgentError({
          operation,
          message: `Codex ${operation} failed; external outcome may be unknown`,
          cause: Redacted.make(cause),
        });
  yield* rpc.events.pipe(
    Stream.runForEach((message) =>
      Effect.gen(function* () {
        if (message.method === "disconnected") {
          controls.clear();
          owned.clear();
          return;
        }
        const params = "params" in message ? (message.params ?? {}) : {};
        if (message.method === "serverRequest/resolved") {
          for (const [id, control] of controls)
            if (control.rpcId === params.requestId) controls.delete(id);
          return;
        }
        if (message.method === "turn/completed") {
          const turn = yield* Schema.decodeUnknownEffect(Schema.optional(Row))(params.turn);
          for (const [id, control] of controls)
            if (control.params.threadId === params.threadId && control.params.turnId === turn?.id)
              controls.delete(id);
          return;
        }
        if ("id" in message && message.id !== undefined && message.method) {
          const id = `${connectionId}:${message.id}`;
          controls.set(id, {
            rpcId: message.id,
            method: message.method,
            params,
            request: yield* decodeControl(id, message.method, params),
          });
        }
      }),
    ),
    Effect.orDie,
    Effect.forkScoped,
  );
  const read = Effect.fn("Codex.status")(function* (
    session: ExecutionSession,
  ): Effect.fn.Return<ExecutionStatus, ExternalAgentError> {
    const requests = [...controls.values()]
      .filter(
        (control) =>
          control.params.threadId === session.sessionId &&
          (!session.runId || !control.params.turnId || control.params.turnId === session.runId),
      )
      .map((control) => control.request);
    if (requests.length) return { state: "waiting_input", requests };
    const response = yield* rpc
      .call("thread/read", { threadId: session.sessionId, includeTurns: true })
      .pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(ThreadRead)),
        Effect.mapError(error("status")),
      );
    const turns = response.thread.turns ?? [];
    const turn = session.runId ? turns.find((item) => item.id === session.runId) : turns.at(-1);
    if (!turn) return { state: "unknown", error: "Codex turn was not found" };
    if (turn.status === "completed")
      return {
        state: "completed",
        result: {
          text: turn.items
            .filter((item) => item.type === "agentMessage")
            .map((item) => item.text ?? "")
            .join("\n"),
        },
      };
    if (turn.status === "failed")
      return {
        state: "failed",
        error: typeof turn.error?.message === "string" ? turn.error.message : "Codex turn failed",
      };
    if (turn.status === "interrupted") {
      const marker =
        typeof session.metadata?.resumeFile === "string"
          ? yield* fs.readFileString(session.metadata.resumeFile).pipe(
              Effect.flatMap(
                Schema.decodeUnknownEffect(
                  Schema.fromJsonString(
                    Schema.Struct({
                      sessionId: Schema.String,
                      runId: Schema.optional(Schema.String),
                    }),
                  ),
                ),
              ),
              Effect.option,
            )
          : undefined;
      if (
        marker?._tag === "Some" &&
        marker.value.sessionId === session.sessionId &&
        marker.value.runId === session.runId
      )
        return {
          state: "failed",
          resumable: true,
          error: "Codex was interrupted by Aster shutdown",
        };
      return {
        state: "cancelled",
        error: "Codex turn was interrupted without evidence of a recoverable shutdown",
      };
    }
    if (turn.status !== "inProgress") return { state: "unknown" };
    if (owned.has(session.sessionId)) return { state: "running" };
    const pid = session.metadata?.processId;
    if (typeof pid !== "number")
      return { state: "unknown", error: "Original Codex process identity is unavailable" };
    const alive = yield* Effect.try({
      try: () => {
        try {
          process.kill(pid, 0);
          return true;
        } catch (cause) {
          if (Predicate.hasProperty(cause, "code") && cause.code === "ESRCH") return false;
          throw cause;
        }
      },
      catch: error("status"),
    });
    return alive
      ? { state: "unknown", error: "Original Codex process is still alive" }
      : {
          state: "failed",
          resumable: true,
          error: "Original Codex process ended during this turn",
        };
  });
  const startTurn = Effect.fnUntraced(function* (session: ExecutionSession, text: string) {
    const response = yield* rpc
      .call("turn/start", {
        threadId: session.sessionId,
        input: [{ type: "text", text, text_elements: [] }],
      })
      .pipe(Effect.flatMap(Schema.decodeUnknownEffect(TurnStart)));
    const next = {
      ...session,
      runId: response.turn.id,
      metadata: { ...session.metadata, processId: yield* rpc.processId },
    };
    sessions.set(next.sessionId, next);
    return next;
  });
  yield* Effect.addFinalizer(() =>
    Effect.forEach(
      [...sessions.values()],
      (session) =>
        read(session).pipe(
          Effect.timeout(1500),
          Effect.flatMap((status) =>
            ["running", "waiting_input"].includes(status.state) &&
            typeof session.metadata?.resumeFile === "string"
              ? fs.writeFileString(
                  session.metadata.resumeFile,
                  JSON.stringify({ sessionId: session.sessionId, runId: session.runId }),
                  { mode: 0o600 },
                )
              : Effect.void,
          ),
          Effect.catchTag(
            ["ExternalAgentError", "TimeoutError", "PlatformError"],
            () => Effect.void,
          ),
        ),
      { discard: true, concurrency: "unbounded" },
    ),
  );
  return {
    capabilities:
      "Execute a self-contained Task in a persistent Codex thread with an isolated writable workspace; native approval and user-input requests are handled through the internal queue.",
    submit: (task) =>
      Effect.gen(function* () {
        const workspace = path.join(taskRoot, yield* Effect.sync(randomUUID));
        yield* fs.makeDirectory(workspace, { recursive: true, mode: 0o700 });
        const response = yield* rpc
          .call("thread/start", {
            cwd: workspace,
            ephemeral: false,
            approvalPolicy: "on-request",
            sandbox: "workspace-write",
          })
          .pipe(Effect.flatMap(Schema.decodeUnknownEffect(ThreadStart)));
        owned.add(response.thread.id);
        return yield* startTurn(
          {
            sessionId: response.thread.id,
            metadata: { workspace, resumeFile: path.join(workspace, ".signals-resume.json") },
          },
          taskPrompt(task),
        );
      }).pipe(Effect.mapError(error("submit"))),
    followUp: (session, input) =>
      Effect.gen(function* () {
        const status = yield* read(session);
        if (["running", "waiting_input"].includes(status.state)) {
          yield* rpc.call("turn/steer", {
            threadId: session.sessionId,
            expectedTurnId: session.runId,
            input: [{ type: "text", text: input.text, text_elements: [] }],
          });
          return session;
        }
        if (status.state === "unknown")
          return yield* new ExternalAgentError({
            operation: "followUp",
            message: "Reconcile the original Codex turn before sending input",
          });
        if (!owned.has(session.sessionId)) {
          yield* rpc.call("thread/resume", {
            threadId: session.sessionId,
            approvalPolicy: "on-request",
            sandbox: "workspace-write",
          });
          owned.add(session.sessionId);
        }
        return yield* startTurn(session, input.text);
      }).pipe(Effect.mapError(error("followUp"))),
    status: read,
    resume: (session) =>
      Effect.gen(function* () {
        const status = yield* read(session);
        if (["running", "completed", "waiting_input"].includes(status.state)) return session;
        if (!status.resumable)
          return yield* new ExternalAgentError({
            operation: "resume",
            outcome: "rejected",
            message: status.error ?? "Codex session cannot be resumed",
          });
        yield* rpc.call("thread/resume", {
          threadId: session.sessionId,
          approvalPolicy: "on-request",
          sandbox: "workspace-write",
        });
        owned.add(session.sessionId);
        return yield* startTurn(
          session,
          "Continue the original task. First check completed actions and existing results to avoid repeating work.",
        );
      }).pipe(Effect.mapError(error("resume"))),
    wait: (session) => Effect.sleep(1000).pipe(Effect.andThen(read(session))),
    respond: (session, request, response) =>
      Effect.gen(function* () {
        const control = controls.get(request.id);
        if (
          !control ||
          control.params.threadId !== session.sessionId ||
          (session.runId && control.params.turnId && control.params.turnId !== session.runId)
        )
          return yield* new ExternalAgentError({
            operation: "respond",
            outcome: "rejected",
            message: "Codex pending request is no longer attached to this session/run",
          });
        yield* rpc.reply(
          control.rpcId,
          yield* controlResponse(control.method, control.params, response),
        );
        controls.delete(request.id);
      }).pipe(Effect.mapError(error("respond"))),
  } satisfies ExternalAgent;
});
