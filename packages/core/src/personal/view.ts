import { PersonalState, PersonalMessage } from "@aster/api-contracts";
import { contextView } from "../context/view.js";
export const personalView = contextView({
  matches: (path) => path === "/personal",
  state: PersonalState,
  message: PersonalMessage,
});
