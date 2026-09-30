import { adaptExternalAgent, type ManagedExternalAgent } from "../external-agent.js";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface } from "node:readline";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import type { ExecutionSession, ExecutionStatus, InputRequest } from "@aster/core";
import { agentEnvironment } from "../process/environment.js";
import { taskPrompt } from "@aster/core";

/** One lazily started app-server connection; native approval requests stay correlated with JSON-RPC IDs. */
export const makeCodexAgent = (
  envPath: string,
  executable = "codex",
  taskRoot = join(homedir(), ".aster", "tasks"),
  environment: NodeJS.ProcessEnv = process.env,
): ManagedExternalAgent => {
  let child: ChildProcessWithoutNullStreams | undefined;
  let initialization: Promise<void> | undefined;
  let sequence = 0;
  const connectionId = randomUUID();
  const pending = new Map<
    number,
    { resolve(value: any): void; reject(error: unknown): void; cleanup(): void }
  >();
  const controls = new Map<
    string,
    { rpcId: string | number; method: string; params: any; request: InputRequest }
  >();
  const owned = new Set<string>();
  const sessions = new Map<string, ExecutionSession>();
  const send = (value: object) => {
    if (!child || child.exitCode !== null) throw new Error("Codex app-server is disconnected");
    child.stdin.write(JSON.stringify(value) + "\n");
  };
  const call = (method: string, params: object, signal?: AbortSignal): Promise<any> =>
    new Promise((resolve, reject) => {
      if (signal?.aborted) {
        reject(signal.reason);
        return;
      }
      const id = ++sequence;
      const cleanup = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", aborted);
        pending.delete(id);
      };
      const aborted = () => {
        cleanup();
        reject(
          new Error(`Codex ${method} cancelled; external outcome may be unknown`, {
            cause: signal?.reason,
          }),
        );
      };
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error(`Codex ${method} timed out; external outcome may be unknown`));
      }, 60_000);
      pending.set(id, { resolve, reject, cleanup });
      signal?.addEventListener("abort", aborted, { once: true });
      try {
        send({ id, method, params });
      } catch (error) {
        cleanup();
        reject(error);
      }
    });
  // Initialization is shared by all callers; cancelling one waiter must not disconnect others.
  const waitFor = <A>(promise: Promise<A>, signal?: AbortSignal): Promise<A> =>
    new Promise((resolve, reject) => {
      const aborted = () => {
        signal?.removeEventListener("abort", aborted);
        reject(signal?.reason);
      };
      signal?.addEventListener("abort", aborted, { once: true });
      promise.then(
        (value) => {
          signal?.removeEventListener("abort", aborted);
          resolve(value);
        },
        (error) => {
          signal?.removeEventListener("abort", aborted);
          reject(error);
        },
      );
      if (signal?.aborted) aborted();
    });
  const ensure = () =>
    (initialization ??= (async () => {
      child = spawn(executable, ["app-server"], {
        env: agentEnvironment(environment, envPath),
        stdio: ["pipe", "pipe", "pipe"],
      });
      const lines = createInterface({ input: child.stdout });
      child.stderr.resume();
      const disconnected = () => {
        for (const item of pending.values()) {
          item.cleanup();
          item.reject(new Error("Codex app-server disconnected"));
        }
        pending.clear();
        controls.clear();
        owned.clear();
        initialization = undefined;
        child = undefined;
      };
      child.once("error", disconnected);
      child.once("exit", disconnected);
      lines.on("line", (line) => {
        let message: any;
        try {
          message = JSON.parse(line);
        } catch {
          return;
        }
        if (message.id !== undefined && !message.method) {
          const item = pending.get(message.id);
          if (!item) return;
          pending.delete(message.id);
          item.cleanup();
          if (message.error)
            item.reject(new Error(message.error.message ?? "Codex request failed"));
          else item.resolve(message.result);
        } else if (message.method === "serverRequest/resolved") {
          for (const [id, item] of controls)
            if (item.rpcId === message.params?.requestId) controls.delete(id);
        } else if (message.method === "turn/completed") {
          for (const [id, item] of controls)
            if (
              item.params.threadId === message.params?.threadId &&
              item.params.turnId === message.params?.turn?.id
            )
              controls.delete(id);
        } else if (message.id !== undefined && message.method) {
          const params = message.params ?? {};
          const kind =
            message.method.includes("requestUserInput") || message.method.includes("elicitation")
              ? "input"
              : "approval";
          const id = `${connectionId}:${message.id}`;
          const request: InputRequest = {
            id,
            kind,
            prompt: JSON.stringify(params),
            ...(Array.isArray(params.questions)
              ? {
                  questions: params.questions.map((question: any) => ({
                    id: question.id,
                    prompt: question.question,
                    allowOther: true,
                    ...(question.options
                      ? { options: question.options.map((option: any) => option.label) }
                      : {}),
                  })),
                }
              : {}),
            metadata: { method: message.method, params },
          };
          controls.set(id, { rpcId: message.id, method: message.method, params, request });
        }
      });
      await call("initialize", {
        clientInfo: { name: "aster", version: "0.0.0" },
        capabilities: { experimentalApi: true },
      });
      send({ method: "initialized", params: {} });
    })());
  const read = async (
    session: ExecutionSession,
    signal?: AbortSignal,
  ): Promise<ExecutionStatus> => {
    signal?.throwIfAborted();
    await waitFor(ensure(), signal);
    signal?.throwIfAborted();
    const requests = [...controls.values()]
      .filter(
        (item) =>
          item.params.threadId === session.sessionId &&
          (!session.runId || !item.params.turnId || item.params.turnId === session.runId),
      )
      .map((item) => item.request);
    if (requests.length) return { state: "waiting_input", requests };
    const result = await call(
      "thread/read",
      { threadId: session.sessionId, includeTurns: true },
      signal,
    );
    const turns = result.thread?.turns ?? [];
    const turn = session.runId
      ? turns.find((turn: any) => turn.id === session.runId)
      : turns.at(-1);
    if (!turn) return { state: "unknown", error: "Codex turn was not found" };
    if (turn.status === "completed") {
      const text = turn.items
        .filter((item: any) => item.type === "agentMessage")
        .map((item: any) => item.text)
        .join("\n");
      return { state: "completed", result: { text } };
    }
    if (turn.status === "interrupted") {
      try {
        const saved = JSON.parse(
          await readFile(String(session.metadata?.resumeFile), { encoding: "utf8", signal }),
        );
        if (saved.sessionId === session.sessionId && saved.runId === session.runId)
          return {
            state: "failed",
            resumable: true,
            error: "Codex was interrupted by Aster shutdown",
          };
      } catch {
        // A missing or invalid marker cannot establish that this turn is recoverable.
      }
      signal?.throwIfAborted();
      return {
        state: "cancelled",
        error: "Codex turn was interrupted without evidence of a recoverable shutdown",
      };
    }
    if (turn.status === "failed")
      return { state: "failed", error: turn.error?.message ?? "Codex turn failed" };
    if (turn.status === "inProgress") {
      if (owned.has(session.sessionId)) return { state: "running" };
      const pid = session.metadata?.processId;
      if (typeof pid !== "number")
        return { state: "unknown", error: "Original Codex process identity is unavailable" };
      try {
        process.kill(pid, 0);
        return {
          state: "unknown",
          error: "Original Codex process is still alive; refusing to start a concurrent turn",
        };
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ESRCH")
          return { state: "unknown", error: "Cannot confirm original Codex process termination" };
        return {
          state: "failed",
          resumable: true,
          error: "Original Codex process ended during this turn",
        };
      }
    }
    return { state: "unknown" };
  };
  return adaptExternalAgent({
    capabilities:
      "Execute a self-contained Task in a persistent Codex thread with an isolated writable workspace; native approval and user-input requests are handled through the internal queue.",
    async submit(task, signal) {
      signal?.throwIfAborted();
      await waitFor(ensure(), signal);
      signal?.throwIfAborted();
      const workspace = join(taskRoot, randomUUID());
      await mkdir(workspace, { recursive: true, mode: 0o700 });
      const { thread } = await call(
        "thread/start",
        {
          cwd: workspace,
          ephemeral: false,
          approvalPolicy: "on-request",
          sandbox: "workspace-write",
        },
        signal,
      );
      owned.add(thread.id);
      const { turn } = await call(
        "turn/start",
        {
          threadId: thread.id,
          input: [{ type: "text", text: taskPrompt(task), text_elements: [] }],
        },
        signal,
      );
      const session = {
        sessionId: thread.id,
        runId: turn.id,
        metadata: {
          workspace,
          processId: child?.pid,
          resumeFile: join(workspace, ".signals-resume.json"),
        },
      };
      sessions.set(session.sessionId, session);
      return session;
    },
    status: read,
    async resume(session, signal) {
      const status = await read(session, signal);
      signal?.throwIfAborted();
      if (
        status.state === "running" ||
        status.state === "completed" ||
        status.state === "waiting_input"
      )
        return session;
      if (!status.resumable) throw new Error(status.error ?? "Codex session cannot be resumed");
      await call(
        "thread/resume",
        {
          threadId: session.sessionId,
          approvalPolicy: "on-request",
          sandbox: "workspace-write",
        },
        signal,
      );
      owned.add(session.sessionId);
      const { turn } = await call(
        "turn/start",
        {
          threadId: session.sessionId,
          input: [
            {
              type: "text",
              text: "Continue the original task. First check completed actions and existing results to avoid repeating work.",
              text_elements: [],
            },
          ],
        },
        signal,
      );
      const resumed = {
        ...session,
        runId: turn.id,
        metadata: { ...session.metadata, processId: child?.pid },
      };
      sessions.set(resumed.sessionId, resumed);
      return resumed;
    },
    async wait(session, signal) {
      await delay(1000, undefined, { signal });
      return read(session, signal);
    },
    async respond(session, request, response, signal) {
      signal?.throwIfAborted();
      await waitFor(ensure(), signal);
      signal?.throwIfAborted();
      const control = controls.get(request.id);
      if (
        !control ||
        control.params.threadId !== session.sessionId ||
        (session.runId && control.params.turnId && control.params.turnId !== session.runId)
      )
        throw new Error("Codex pending request is no longer attached to this session/run");
      let result: object;
      if (control.method === "item/tool/requestUserInput") {
        const questions = control.params.questions ?? [];
        const answers =
          response.answers ??
          (questions.length === 1 && response.text ? { [questions[0].id]: [response.text] } : {});
        if (questions.some((q: any) => !answers[q.id]?.length))
          throw new Error("Every Codex question requires an answer keyed by question ID");
        result = {
          answers: Object.fromEntries(
            Object.entries(answers).map(([key, answers]) => [key, { answers }]),
          ),
        };
      } else if (
        ["item/commandExecution/requestApproval", "item/fileChange/requestApproval"].includes(
          control.method,
        )
      ) {
        if (!response.decision) throw new Error("An approval decision is required");
        result = { decision: response.decision === "approve" ? "accept" : "decline" };
      } else if (control.method === "item/permissions/requestApproval") {
        if (!response.decision) throw new Error("An approval decision is required");
        result = {
          permissions: response.decision === "approve" ? control.params.permissions : {},
          scope: "turn",
        };
      } else throw new Error(`Unsupported Codex control: ${control.method}`);
      signal?.throwIfAborted();
      send({ id: control.rpcId, result });
      controls.delete(request.id);
    },
    async close() {
      const current = child;
      if (!current) return;
      // Record only work observed as active at this application's shutdown, not user-cancelled turns.
      await Promise.all(
        [...sessions.values()].map(async (session) => {
          let timer: NodeJS.Timeout | undefined;
          try {
            const status = await Promise.race([
              read(session),
              new Promise<undefined>((resolve) => {
                timer = setTimeout(() => resolve(undefined), 1500);
              }),
            ]);
            if (
              status &&
              ["running", "waiting_input"].includes(status.state) &&
              typeof session.metadata?.resumeFile === "string"
            ) {
              await writeFile(
                session.metadata.resumeFile,
                JSON.stringify({ sessionId: session.sessionId, runId: session.runId }),
                { mode: 0o600 },
              );
            }
          } catch {
            // Resume markers are best effort; shutdown must still stop the child process.
          } finally {
            clearTimeout(timer);
          }
        }),
      );
      if (current.exitCode !== null || current.signalCode !== null) return;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => current.kill("SIGKILL"), 2000);
        current.once("exit", () => {
          clearTimeout(timer);
          resolve();
        });
        current.kill("SIGTERM");
      });
    },
  });
};
