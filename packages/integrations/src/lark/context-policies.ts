import type { CapturePolicy } from "@aster/core";
import { Effect, Option, Schema } from "effect";
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
        ? {
            sessionId: profileCapture(record, decoded.value.account),
            records: Effect.succeed([record]),
          }
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
        ? {
            sessionId: profileCapture(record, decoded.value.profile),
            records: Effect.succeed([record]),
          }
        : undefined;
    },
  },
];
