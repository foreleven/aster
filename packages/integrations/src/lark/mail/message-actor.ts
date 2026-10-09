import { Command as ActorCommand } from "@aster/actor";
import { Effect, Match, Schema } from "effect";
import { emailView } from "../public-views.js";

import { ContextActor, ContextRegistry, defineContext } from "@aster/core";
import { EmailData } from "./model.js";
export class MailMessageCommand extends ActorCommand.Class<MailMessageCommand>()("SetEmail", {
  payload: { email: EmailData },
  reply: Schema.Void,
}) {}

export const LarkMailMessageActor = ContextActor.define("lark/MailMessageActor", {
  commands: [MailMessageCommand],
  context: defineContext({
    view: emailView,
    state: EmailData,
    message: Schema.Never,
    changes: "durable-state",
  }),
})(
  Effect.gen(function* () {
    const registry = yield* ContextRegistry;
    return {
      started: (context) => context.receiveTimeout("5 minutes"),
      receive: (command) =>
        Match.value(command).pipe(
          Match.tag("SetEmail", ({ email, replyTo }) => {
            const path = `/lark/mail/${email.mailbox}/${email.messageId}`;
            const previous = registry.get(path);
            return registry
              .commit(
                {
                  path,
                  description: `An email in Lark mailbox ${email.mailbox}`,
                  state: email,
                  messages: [],
                },
                { expectedRevision: previous?.revision ?? 0 },
              )
              .pipe(Effect.orDie, Effect.andThen(replyTo.tell(undefined)));
          }),
          Match.exhaustive,
        ),
    };
  }),
);
