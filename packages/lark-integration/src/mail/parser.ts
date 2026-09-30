import { object, string, parseCliOutput } from "../shared/response.js";
import type { EmailData, MailboxProfile } from "./model.js";
export const parseRecentIds = (stdout: string): ReadonlyArray<string> => {
  const data = parseCliOutput(stdout);
  if (!Array.isArray(data.messages)) throw new Error("lark-cli did not return a messages array");
  return data.messages.map((entry) => string(object(entry).message_id)).filter(Boolean);
};

export const parseMessages = (stdout: string, mailbox: string): ReadonlyArray<EmailData> => {
  const data = parseCliOutput(stdout);
  if (!Array.isArray(data.messages)) throw new Error("lark-cli did not return a messages array");
  return data.messages
    .map((entry) => {
      const message = object(entry);
      const from = object(message.head_from);
      const attachments = Array.isArray(message.attachments) ? message.attachments : [];
      return {
        messageId: string(message.message_id),
        mailbox,
        from: [string(from.name), string(from.mail_address)].filter(Boolean).join(" "),
        subject: string(message.subject),
        bodyPlainText: string(message.body_plain_text),
        attachments: attachments.map((entry) => {
          const attachment = object(entry);
          return {
            id: string(attachment.id),
            filename: string(attachment.filename),
            contentType: string(attachment.content_type),
            isInline: attachment.is_inline === true,
          };
        }),
      };
    })
    .filter((message) => message.messageId !== "");
};

export const parseMailboxProfile = (stdout: string): MailboxProfile => {
  const data = parseCliOutput(stdout);
  const profile = object(data.profile ?? data.user_mailbox ?? data);
  const address = string(profile.primary_email_address ?? profile.mail_address ?? profile.email);
  if (!address) throw new Error("lark-cli did not return a mailbox address");
  return { address, name: string(profile.name ?? profile.display_name) };
};
