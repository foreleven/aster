import { LarkCliError } from "../shared/errors.js";
import { Context, Data, Effect } from "effect";
import { runLarkCli } from "../shared/cli.js";
import type { EmailData, MailboxProfile } from "./model.js";
import { parseRecentIds, parseMessages, parseMailboxProfile } from "./parser.js";
export class LarkMailCli extends Context.Service<
  LarkMailCli,
  {
    readonly getMailboxProfile: (
      mailbox: string,
    ) => Effect.Effect<MailboxProfile, LarkCliError | LarkResponseError>;
    readonly listRecentIds: (
      mailbox: string,
    ) => Effect.Effect<ReadonlyArray<string>, LarkCliError | LarkResponseError>;
    readonly getMessages: (
      mailbox: string,
      ids: ReadonlyArray<string>,
    ) => Effect.Effect<ReadonlyArray<EmailData>, LarkCliError | LarkResponseError>;
  }
>()("lark/MailCli") {}

export class LarkResponseError extends Data.TaggedError("LarkResponseError")<{
  readonly cause: unknown;
}> {
  override get message() {
    return `Invalid Lark mail response: ${String(this.cause)}`;
  }
}
const decode =
  <A>(parse: (stdout: string) => A) =>
  (stdout: string) =>
    Effect.try({ try: () => parse(stdout), catch: (cause) => new LarkResponseError({ cause }) });

export const liveCli = (profile?: string) => {
  const run = (args: ReadonlyArray<string>) =>
    Effect.tryPromise({
      try: async (signal) => {
        return await runLarkCli(args, profile, signal);
      },
      catch: (cause) => new LarkCliError({ cause, message: `lark-cli failed: ${String(cause)}` }),
    });
  return {
    getMailboxProfile: (mailbox: string) =>
      run([
        "mail",
        "user_mailboxes",
        "profile",
        "--as",
        "user",
        "--params",
        JSON.stringify({ user_mailbox_id: mailbox }),
      ]).pipe(Effect.flatMap(decode(parseMailboxProfile))),
    listRecentIds: (mailbox: string) =>
      run([
        "mail",
        "+triage",
        "--as",
        "user",
        "--mailbox",
        mailbox,
        "--folder",
        "INBOX",
        "--max",
        "100",
        "--format",
        "json",
      ]).pipe(Effect.flatMap(decode(parseRecentIds))),
    getMessages: (mailbox: string, ids: ReadonlyArray<string>) =>
      ids.length === 0
        ? Effect.succeed([])
        : run([
            "mail",
            "+messages",
            "--as",
            "user",
            "--mailbox",
            mailbox,
            "--message-ids",
            ids.join(","),
            "--html=false",
            "--format",
            "json",
          ]).pipe(Effect.flatMap(decode((stdout) => parseMessages(stdout, mailbox)))),
  };
};
