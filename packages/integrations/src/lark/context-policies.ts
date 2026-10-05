import type { CapturePolicy, DescriptionPolicy } from "@aster/core";
import { Option, Schema } from "effect";
import { AccountProfile } from "./account/model.js";
import { MailboxProfile } from "./mail/model.js";
import { profileCapture } from "./shared/profile.js";

export const larkCaptures: readonly CapturePolicy[] = [
  {
    matches: (path) => path === "/lark",
    capture: (record) => {
      const decoded = Schema.decodeUnknownOption(Schema.Struct({ account: AccountProfile }))(
        record.state,
      );
      return Option.isSome(decoded)
        ? { sessionId: profileCapture(record, decoded.value.account), records: [record] }
        : undefined;
    },
  },
  {
    matches: (path) => path === "/lark/mail",
    capture: (record) => {
      const decoded = Schema.decodeUnknownOption(Schema.Struct({ profile: MailboxProfile }))(
        record.state,
      );
      return Option.isSome(decoded)
        ? { sessionId: profileCapture(record, decoded.value.profile), records: [record] }
        : undefined;
    },
  },
];
export const larkDescriptions: readonly DescriptionPolicy[] = [
  { matches: (path) => path === "/lark", identity: "Lark account" },
  { matches: (path) => path === "/lark/mail", identity: "Lark mailbox" },
  { matches: (path) => path.startsWith("/lark/mail/"), identity: "An email in a Lark mailbox" },
  { matches: (path) => path === "/lark/im", identity: "Work Lark IM integration" },
  { matches: (path) => path.startsWith("/lark/im/"), identity: "Work Lark conversation" },
];
