import { Schema } from "effect";
export const AccountProfile = Schema.Struct({
  openId: Schema.String,
  name: Schema.String,
  email: Schema.String,
  enterpriseEmail: Schema.String,
});
export type AccountProfile = typeof AccountProfile.Type;
