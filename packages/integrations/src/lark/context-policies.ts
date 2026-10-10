import type { CapturePolicy } from "@aster/core";
import { Effect } from "effect";
import { profileCapture } from "./shared/profile.js";

export const larkCaptures: readonly CapturePolicy[] = ["/lark", "/lark/mail"].map((path) => ({
  matches: (candidate) => candidate === path,
  capture: (record) =>
    record.text.trim()
      ? { sessionId: profileCapture(record, record.text), records: Effect.succeed([record]) }
      : undefined,
}));
