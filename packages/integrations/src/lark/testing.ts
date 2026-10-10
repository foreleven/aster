/** Internal fixtures only; production consumers use the package root. */
export * from "./index.js";
export { ChatPollingConfig, AgentAdmissionConfig, ChatSummaryConfig } from "./settings.js";
export { makeLarkRootActor } from "./account/actor.js";
export { makeAccountClient } from "./account/client.js";
export { LarkMailActor } from "./mail/actor.js";
export { LarkEmailActor, SetEmail } from "./mail/email-actor.js";
export { makeMailClient } from "./mail/client.js";
export { LarkImActor } from "./im/actor.js";
export { LarkChatActor, Update, Flush } from "./im/chat/actor.js";
export { ChatState, SummaryCommit } from "./im/chat/state.js";
export {
  ChatSummarizer,
  makeChatSummarizer,
  type ChatSummaryInput,
} from "./im/summary/summarizer.js";
export { ChatSummaryGate, makeImSummaryGate } from "./im/summary/gate.js";
export {
  SummaryAdmission,
  makeSummaryAdmission,
  type AgentAdmission,
} from "./im/summary/admission.js";
export { ChatSummaryError } from "./im/summary/errors.js";
export { pollChats } from "./im/polling.js";
export { mailWindow, mailDayStart } from "./mail/polling.js";
export { formatDate, dayStart, nextDay } from "./dates.js";
export { larkCaptures } from "./captures.js";

import { Effect } from "effect";
import { decodeLarkSettings } from "./settings.js";
/** Synchronous fixture setup is confined to test runners. */
export const larkSettings = (entry: unknown) => Effect.runSync(decodeLarkSettings(entry));
