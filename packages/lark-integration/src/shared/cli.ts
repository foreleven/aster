import { execFile } from "node:child_process";
import { promisify } from "node:util";
const exec = promisify(execFile);
export const runLarkCli = async (
  args: readonly string[],
  profile?: string,
  signal?: AbortSignal,
): Promise<string> =>
  (
    await exec("lark-cli", [...(profile ? ["--profile", profile] : []), ...args], {
      signal,
      timeout: 60_000,
      maxBuffer: 32 * 1024 * 1024,
      env: {
        ...process.env,
        LARKSUITE_CLI_NO_UPDATE_NOTIFIER: "1",
        LARKSUITE_CLI_NO_SKILLS_NOTIFIER: "1",
      },
    })
  ).stdout;
