import { Effect, Match, Layer, Schema } from "effect";

import { ContextActor, ContextRegistry, defineContext } from "@aster/core";
import { EmailData } from "./model.js";
const MailMessageCommand = Schema.TaggedStruct("SetEmail", {
  email: EmailData,
});
export type MailMessageCommand = typeof MailMessageCommand.Type;

export class LarkMailMessageActor extends ContextActor.Service<LarkMailMessageActor>()(
  "lark/MailMessageActor",
  {
    command: MailMessageCommand,
    context: defineContext({
      identity: "An email in a Lark mailbox",
      state: EmailData,
      message: Schema.Never,
      signalSource: true,
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
            Match.tag("SetEmail", ({ email }) => {
              const path = `/lark/mail/${email.mailbox}/${email.messageId}`;
              return registry.set({
                path,
                description: "",
                state: email,
                messages: [],
              });
            }),
            Match.exhaustive,
          ),
      });
    }),
  );
}
