import { ContextCommand, ContextQueryError } from "@aster/core";

import { AccountProfile } from "./model.js";

export class AccountProfileQuery extends ContextCommand.Class<AccountProfileQuery>()("profile", {
  description: "Read the connected account's public identity from the Account Actor.",
  payload: {},
  success: AccountProfile,
  error: ContextQueryError,
}) {}
export const LarkAccountCommands = [AccountProfileQuery] as const;
