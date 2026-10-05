import { ReplyTo } from "@aster/actor";
import { emailView } from "../public-views.js";
import { Effect, Match, Layer, Schema } from "effect";

import { ContextActor, ContextRegistry, defineContext } from "@aster/core";
import { EmailData } from "./model.js";
const MailMessageCommand = Schema.TaggedStruct("SetEmail", {
  email: EmailData,
  replyTo: ReplyTo<void>(),
});
export type MailMessageCommand = typeof MailMessageCommand.Type;

export class LarkMailMessageActor extends ContextActor.Service<LarkMailMessageActor>()(
  "lark/MailMessageActor",
  {
    command: MailMessageCommand,
    context: defineContext({
      view: emailView,
      state: EmailData,
      message: Schema.Never,
      changes: "durable-state",
    }),
  },
) {
  static readonly layer = Layer.effect(
    LarkMailMessageActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      return LarkMailMessageActor.of({
        started: (context) => context.receiveTimeout("5 minutes"),
        receive: (command) =>
          Match.value(command).pipe(
            Match.tag("SetEmail", ({ email, replyTo }) => {
              const path = `/lark/mail/${email.mailbox}/${email.messageId}`;
              return registry
                .commit(
                  {
                    path,
                    description: "",
                    state: email,
                    messages: [],
                  },
                  { expectedRevision: registry.get(path)?.revision ?? 0 },
                )
                .pipe(Effect.orDie, Effect.andThen(replyTo.tell(undefined)));
            }),
            Match.exhaustive,
          ),
      });
    }),
  );
}
