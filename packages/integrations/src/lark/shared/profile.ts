import { createHash } from "node:crypto";
export const profileCapture = (record: { readonly path: string }, profile: unknown) =>
  `profile:${record.path}:${createHash("sha256").update(JSON.stringify(profile)).digest("hex")}`;
