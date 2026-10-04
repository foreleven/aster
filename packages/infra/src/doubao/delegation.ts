import { decodeDoubaoReceipt, doubaoStatus } from "./status.js";
import { adaptExternalAgent, type ManagedExternalAgent } from "../external-agent.js";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { DEFAULT_EXECUTOR_PROMPT, taskPrompt } from "@aster/core";
import type { ExecutionSession } from "@aster/core";
import { respondDoubaoNative } from "./native-response.js";
import { agentEnvironment } from "../process/environment.js";

const exec = promisify(execFile);
export type DoubaoCommand = (args: string[], signal?: AbortSignal) => Promise<unknown>;
export { doubaoStatus } from "./status.js";
export const makeDoubaoAgent = (
  envPath?: string,
  command?: DoubaoCommand,
  respond = respondDoubaoNative,
  prompt = DEFAULT_EXECUTOR_PROMPT,
  environment: NodeJS.ProcessEnv = process.env,
): ManagedExternalAgent => {
  let queue: Promise<unknown> = Promise.resolve();
  const run: DoubaoCommand =
    command ??
    ((args, signal) => {
      const operation = queue.then(async () => {
        signal?.throwIfAborted();
        try {
          const { stdout } = await exec("doubao", ["--app", "work", ...args, "--json"], {
            signal,
            timeout: 90_000,
            maxBuffer: 16 * 1024 * 1024,
            env: agentEnvironment(environment, envPath),
          });
          return JSON.parse(stdout);
        } catch (error) {
          // CLI wait returns a nonzero exit for waiting_input/failure and a useful JSON envelope.
          const stdout = (error as { stdout?: string }).stdout;
          if (stdout) {
            try {
              const value = JSON.parse(stdout);
              if (typeof value.status === "string") return value;
            } catch {
              // Non-JSON output carries no structured status; preserve the original CLI error.
            }
          }
          throw error;
        }
      });
      queue = operation.catch(() => undefined);
      return operation;
    });
  const args = (session: ExecutionSession) => [
    session.sessionId,
    ...(session.runId ? ["--run", session.runId] : []),
  ];
  return adaptExternalAgent({
    executorPrompt: prompt,
    capabilities:
      "Execute a self-contained Task in a Doubao Work session with a local workspace. Requests for additional authorization or information are surfaced for human handling.",
    async submit(task, signal) {
      signal.throwIfAborted();
      await run(["cdp", "launch"], signal);
      signal.throwIfAborted();
      const workspace = join(homedir(), ".aster", "tasks", randomUUID());
      await mkdir(workspace, { recursive: true, mode: 0o700 });
      signal.throwIfAborted();
      const receipt = decodeDoubaoReceipt(
        await run(
          [
            "sessions",
            "create",
            `${prompt}\n\n${taskPrompt(task)}`,
            "--workspace",
            workspace,
            "--runtime",
            "local",
            "--permission",
            "AskOnRisk",
            "--no-skills",
          ],
          signal,
        ),
      );
      return {
        sessionId: String(receipt.conversationId),
        runId: String(receipt.runId),
        metadata: { workspace },
      };
    },
    async status(session, signal) {
      return doubaoStatus(await run(["sessions", "status", ...args(session)], signal));
    },
    async wait(session, signal) {
      try {
        return doubaoStatus(
          await run(["sessions", "wait", ...args(session), "--timeout", "30"], signal),
        );
      } catch (error) {
        if (signal?.aborted) throw error;
        // CLI timeout does not cancel the external run. Re-query its authoritative status.
        return doubaoStatus(await run(["sessions", "status", ...args(session)], signal));
      }
    },
    async resume(session, signal) {
      const status = doubaoStatus(await run(["sessions", "status", ...args(session)], signal));
      // The CLI wait reconnects the original run/receipt; no new conversational turn is sent.
      if (["failed", "cancelled", "unknown"].includes(status.state))
        throw new Error(`Doubao run cannot be resumed: ${status.state}`);
      return session;
    },
    async respond(session, request, response, signal) {
      const current = doubaoStatus(await run(["sessions", "status", ...args(session)], signal));
      const pending = current.requests?.find((item) => item.id === request.id);
      if (current.state !== "waiting_input" || !pending)
        throw new Error("Original external approval is no longer pending");
      signal.throwIfAborted();
      await respond(pending.metadata, response, signal);
    },
  });
};
