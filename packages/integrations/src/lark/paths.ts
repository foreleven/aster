import { Schema } from "effect";

/** Opaque provider IDs form exactly one Context segment. */
export const LarkId = Schema.NonEmptyString.check(Schema.isPattern(/^(?!\.\.?$)[^/\\\0]+$/));
export const chatPath = (id: string) => `/lark/im/chats/${id}`;
export const emailPath = (mailbox: string, id: string) => `/lark/mail/${mailbox}/${id}`;
export const isChatPath = (path: string) => /^\/lark\/im\/chats\/[^/]+$/.test(path);
