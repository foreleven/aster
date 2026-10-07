import { posix } from "node:path";
import type { Context as ChordContext } from "@earendil-works/chord";
import {
  ExecutionError,
  FileError,
  err,
  ok,
  type ExecutionEnv,
  type FileInfo,
  type Result,
  type TextLineReader,
} from "@earendil-works/pi-durable/env";

export const evidencePolicyId = "aster.prepared-evidence.v1";
export interface SandboxEvidence {
  readonly namespace: string;
  readonly prompt: string;
  readonly instructions: string;
}

/** Native SDK boundary. This filesystem is an immutable value, not a path filter
 * around the host filesystem. It has no host I/O, process, network or credential
 * capability to delegate. Changing cwd cannot change the visible namespace. */
export const evidenceEnvironment = (input: SandboxEvidence): ExecutionEnv => {
  const files = new Map([
    ["/task/input.md", input.prompt],
    ["/task/instructions.md", input.instructions],
  ]);
  const denied = <A>(): Result<A, FileError> =>
    err(
      new FileError(
        "permission_denied",
        "The execution policy permits only prepared task evidence",
      ),
    );
  const aborted = <A>(): Result<A, FileError> =>
    err(new FileError("aborted", "Execution environment operation was cancelled"));
  const path = (value: string, context: ChordContext): Result<string, FileError> => {
    if (context.abortSignal?.aborted) return aborted();
    if (value.includes("\0") || value.includes("\\") || value.startsWith("~")) return denied();
    const absolute = posix.resolve("/task", value);
    return absolute === "/task" || files.has(absolute) ? ok(absolute) : denied();
  };
  const read = (value: string, context: ChordContext): Result<string, FileError> => {
    const resolved = path(value, context);
    if (!resolved.ok) return resolved;
    const content = files.get(resolved.value);
    return content === undefined
      ? err(new FileError("is_directory", "The requested evidence path is a directory"))
      : ok(content);
  };
  const info = (value: string): FileInfo => ({
    name: posix.basename(value),
    path: value,
    kind: value === "/task" ? "directory" : "file",
    size: new TextEncoder().encode(files.get(value) ?? "").byteLength,
    // Evidence identity comes from durable admission, not wall-clock time.
    mtimeMs: 0,
  });
  const denyWrite = async (): Promise<Result<never, FileError>> => denied();
  return {
    id: JSON.stringify([evidencePolicyId, input.namespace]),
    cwd: "/task",
    absolutePath: async (value, context) => path(value, context),
    joinPath: async (parts, context) => path(posix.join(...parts), context),
    canonicalPath: async (value, context) => path(value, context),
    readTextFile: async (value, context) => read(value, context),
    readBinaryFile: async (value, context) => {
      const result = read(value, context);
      return result.ok ? ok(new TextEncoder().encode(result.value)) : result;
    },
    readTextLines: async (value, options, context) => {
      const result = read(value, context);
      if (!result.ok) return result;
      const lines = result.value.split("\n");
      return ok(options?.maxLines === undefined ? lines : lines.slice(0, options.maxLines));
    },
    openTextLineReader: async (value, context) => {
      const result = read(value, context);
      if (!result.ok) return result;
      // Native reader cursor is private to this call; it cannot mutate evidence.
      let offset = 0;
      const reader: TextLineReader = {
        readLine: async (context) => {
          if (context.abortSignal?.aborted) return aborted();
          if (offset >= result.value.length) return ok(undefined);
          const end = result.value.indexOf("\n", offset);
          const terminated = end !== -1;
          const text = result.value.slice(offset, terminated ? end : undefined).replace(/\r$/, "");
          offset = terminated ? end + 1 : result.value.length;
          return ok({ text, terminated });
        },
        close: async () => {
          offset = result.value.length;
        },
      };
      return ok(reader);
    },
    fileInfo: async (value, context) => {
      const result = path(value, context);
      return result.ok ? ok(info(result.value)) : result;
    },
    listDir: async (value, context) => {
      const result = path(value, context);
      if (!result.ok) return result;
      return result.value === "/task"
        ? ok([...files.keys()].map(info))
        : err(new FileError("not_directory", "The requested evidence path is a file"));
    },
    exists: async (value, context) => {
      const result = path(value, context);
      return result.ok ? ok(true) : result;
    },
    writeFile: denyWrite,
    appendFile: denyWrite,
    truncateFile: denyWrite,
    flushFile: denyWrite,
    renameFile: denyWrite,
    createDir: denyWrite,
    remove: denyWrite,
    createTempDir: denyWrite,
    createTempFile: denyWrite,
    exec: async (_command, _options, context) =>
      err(
        new ExecutionError(
          context.abortSignal?.aborted ? "aborted" : "shell_unavailable",
          "Process execution, network access and credential access are unavailable under the prepared-evidence policy",
        ),
      ),
    // No resources or temporary host files exist to release.
    cleanup: async () => {},
  };
};
