import { object, string, parseCliOutput } from "../shared/response.js";
import type { AccountProfile } from "./model.js";
export const parseAccount = (stdout: string): AccountProfile => {
  const data = parseCliOutput(stdout);
  const user = object(data.user ?? data);
  const openId = string(user.open_id);
  const name = string(user.name);
  if (!openId && !name) throw new Error("lark-cli did not return an account profile");
  return {
    openId,
    name,
    email: string(user.email),
    enterpriseEmail: string(user.enterprise_email),
  };
};
