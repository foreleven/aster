import { Schema } from "effect";
export const AccountProfile = Schema.Struct({
  openId: Schema.String,
  name: Schema.String,
  email: Schema.String,
  enterpriseEmail: Schema.String,
});
export type AccountProfile = typeof AccountProfile.Type;

export const AccountState = Schema.Struct({ account: Schema.optional(AccountProfile) });
export type AccountState = typeof AccountState.Type;
export const serializeAccount = ({ account }: Readonly<AccountState>) =>
  account ? JSON.stringify(account) : "";
