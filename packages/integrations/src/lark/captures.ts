import type { CapturePolicy } from "@aster/core";
import { Effect } from "effect";
import { createHash } from "node:crypto";
const profileCapture = (record: { readonly path: string }, profile: unknown) =>
  `profile:${record.path}:${createHash("sha256").update(JSON.stringify(profile)).digest("hex")}`;

export const larkCaptures = (mail: boolean): readonly CapturePolicy[] =>
  ["/lark", ...(mail ? ["/lark/mail"] : [])].map((path) => ({
    matches: (candidate) => candidate === path,
    capture: (record) =>
      record.text.trim()
        ? { sessionId: profileCapture(record, record.text), records: Effect.succeed([record]) }
        : undefined,
  }));
