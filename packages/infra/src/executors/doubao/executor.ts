import { HttpClient } from "effect/http";
import { randomUUID } from "node:crypto";
import { Effect, FileSystem, Path, Redacted } from "effect";
import {
  taskPrompt,
  ExternalAgentError,
  type ExternalAgent,
  type ExecutionSession,
} from "@aster/core";
import { makeDoubaoCli, type DoubaoCommand } from "./cli.js";
import { decodeDoubaoReceipt, doubaoStatus } from "./protocol.js";
import { respondDoubaoNative } from "./approval.js";
import type { ApprovalResponse } from "@aster/core";

export const makeDoubaoExecutor = Effect.fn("Doubao.executor")(function* (
  environment: Readonly<Record<string, string | undefined>>,
  settings: { readonly prompt: string; readonly endpoint: string; readonly taskRoot: string },
  command?: DoubaoCommand,
  nativeResponse?: (
    request: unknown,
    response: ApprovalResponse,
  ) => Effect.Effect<void, ExternalAgentError>,
) {
  const http = yield* HttpClient.HttpClient;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const run = command ?? (yield* makeDoubaoCli(environment));
  const respond =
    nativeResponse ??
    ((request, response) =>
      respondDoubaoNative(request, response, settings.endpoint).pipe(
        Effect.provideService(HttpClient.HttpClient, http),
      ));
  const args = (session: ExecutionSession) => [
    session.sessionId,
    ...(session.runId ? ["--run", session.runId] : []),
  ];
  const error = (operation: ExternalAgentError["operation"]) => (cause: unknown) =>
    cause instanceof ExternalAgentError
      ? cause
      : new ExternalAgentError({
          operation,
          message: `Doubao ${operation} failed; external outcome may be unknown`,
          cause: Redacted.make(cause),
        });
  const status = (session: ExecutionSession) =>
    run(["sessions", "status", ...args(session)]).pipe(
      Effect.flatMap((value) =>
        Effect.try({ try: () => doubaoStatus(value), catch: error("status") }),
      ),
      Effect.mapError(error("status")),
    );
  return {
    capabilities:
      "Execute a self-contained Task in a Doubao Work session with a local workspace. Requests for additional authorization or information are surfaced for human handling.",
    executorPrompt: settings.prompt,
    submit: (task) =>
      Effect.gen(function* () {
        yield* run(["cdp", "launch"]);
        const workspace = path.join(settings.taskRoot, yield* Effect.sync(randomUUID));
        yield* fs.makeDirectory(workspace, { recursive: true, mode: 0o700 });
        const receipt = yield* run([
          "sessions",
          "create",
          taskPrompt(task),
          "--workspace",
          workspace,
          "--runtime",
          "local",
          "--permission",
          "AskOnRisk",
          "--no-skills",
        ]).pipe(
          Effect.flatMap((value) =>
            Effect.try({ try: () => decodeDoubaoReceipt(value), catch: error("submit") }),
          ),
        );
        return { sessionId: receipt.conversationId, runId: receipt.runId, metadata: { workspace } };
      }).pipe(Effect.mapError(error("submit"))),
    followUp: (_session: ExecutionSession, _input: { requestId: string; text: string }) =>
      Effect.fail(
        new ExternalAgentError({
          operation: "followUp",
          outcome: "rejected",
          message: "The configured Doubao adapter does not support follow-up instructions",
        }),
      ),
    status,
    wait: (session) =>
      run(["sessions", "wait", ...args(session), "--timeout", "30"]).pipe(
        Effect.flatMap((value) =>
          Effect.try({ try: () => doubaoStatus(value), catch: error("wait") }),
        ),
        Effect.catchTag("DoubaoWaitTimeout", () => status(session)),
        Effect.mapError(error("wait")),
      ),
    resume: (session) =>
      status(session).pipe(
        Effect.flatMap((current) =>
          ["failed", "cancelled", "unknown"].includes(current.state)
            ? Effect.fail(
                new ExternalAgentError({
                  operation: "resume",
                  outcome: "rejected",
                  message: `Doubao run cannot be resumed: ${current.state}`,
                }),
              )
            : Effect.succeed(session),
        ),
      ),
    respond: (session, request, response) =>
      status(session).pipe(
        Effect.flatMap((current) => {
          const pending = current.requests?.find((item) => item.id === request.id);
          return current.state !== "waiting_input" || !pending
            ? Effect.fail(
                new ExternalAgentError({
                  operation: "respond",
                  outcome: "rejected",
                  message: "Original external approval is no longer pending",
                }),
              )
            : respond(pending.metadata, response);
        }),
      ),
  } satisfies ExternalAgent;
});
