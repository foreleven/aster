import { LarkAccountError } from "../shared/errors.js";
import { Context, Effect } from "effect";
import { runLarkCli } from "../shared/cli.js";
import { parseAccount } from "./parser.js";
import type { AccountProfile } from "./model.js";
export class LarkAccountCli extends Context.Service<
  LarkAccountCli,
  {
    readonly getAccount: () => Effect.Effect<AccountProfile, LarkAccountError>;
  }
>()("lark/AccountCli") {}
export const makeAccountClient = (profile?: string): LarkAccountCli["Service"] => ({
  getAccount: () =>
    Effect.tryPromise({
      try: async (signal) =>
        parseAccount(await runLarkCli(["contact", "+get-user", "--as", "user"], profile, signal)),
      catch: (cause) =>
        cause instanceof LarkAccountError
          ? cause
          : new LarkAccountError({
              cause,
              message: cause instanceof Error ? cause.message : String(cause),
            }),
    }),
});
