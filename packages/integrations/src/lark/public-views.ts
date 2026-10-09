import { Schema } from "effect";
import { contextView } from "@aster/core";
import { AccountProfile } from "./account/model.js";
import { ChatInfo, ChatPublicMessage } from "./im/service/model.js";
import { ChatSummary } from "./im/service/model.js";
import { EmailData, MailboxProfile } from "./mail/model.js";

export const accountView = contextView({
  matches: (path) => path === "/lark",
  state: Schema.Struct({ account: Schema.optional(AccountProfile) }),
});
export const imChannelView = contextView({
  matches: (path) => path === "/lark/im",
  state: Schema.Struct({
    ready: Schema.Boolean,
    chats: Schema.Number,
    lastError: Schema.optional(Schema.String),
  }),
});
export const chatView = contextView({
  matches: (path) => /^\/lark\/im\/chats\/[^/]+$/.test(path),
  state: Schema.Struct({ chat: ChatInfo, summary: Schema.optional(ChatSummary) }),
  description: ({ chat }) =>
    [`Work Lark conversation: ${chat.name.trim() || chat.id}`, chat.description.trim()]
      .filter(Boolean)
      .join("\n"),
  message: ChatPublicMessage,
});
export const mailChannelView = contextView({
  matches: (path) => path === "/lark/mail",
  state: Schema.Struct({ mailbox: Schema.String, profile: Schema.optional(MailboxProfile) }),
});
export const emailView = contextView({
  matches: (path) => /^\/lark\/mail\/[^/]+\/[^/]+$/.test(path),
  state: EmailData,
});
export const larkContextViews = [accountView, imChannelView, chatView, mailChannelView, emailView];
