import { ContextCommand, ContextQueryError, DurableContext } from "@aster/core";
import { Effect } from "effect";
import { queryError } from "../errors.js";
import { AccountState, AccountProfile } from "./model.js";

export class AccountProfileQuery extends ContextCommand.Class<AccountProfileQuery>()("profile", {
  description: "Read the connected account's public identity from committed local state.",
  payload: {},
  success: AccountProfile,
  error: ContextQueryError,
}) {}
export const LarkAccountCommands = [AccountProfileQuery] as const;

export const readAccountProfile = Effect.fn("LarkAccount.readProfile")(function* () {
  const state = yield* (yield* DurableContext)
    .readState("/lark", AccountState)
    .pipe(Effect.catchTag("ContextValidationError", (error) => Effect.fail(queryError(error))));
  if (!state.account)
    return yield* new ContextQueryError({
      kind: "unavailable",
      message: "Account profile is not available",
    });
  return state.account;
});
