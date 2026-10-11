import { ContextCommand } from "../queries/commands.js";
import { ContextQueryError } from "../queries/contracts.js";
import { UserProfile } from "./model.js";

export class ReadUserProfile extends ContextCommand.Class<ReadUserProfile>()("profile", {
  description: "Read the user's explicit profile and stable response preferences.",
  payload: {},
  success: UserProfile,
  error: ContextQueryError,
}) {
  static override text = (profile: UserProfile): string => JSON.stringify(profile, null, 2);
}
