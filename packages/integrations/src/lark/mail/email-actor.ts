import { CurrentContextPath } from "@aster/core";
import { Command } from "@aster/actor";
import { ContextActor, DurableContext, contextPath } from "@aster/core";
import { Effect, Schema } from "effect";
import { EmailData } from "./model.js";
import { emailPath } from "../paths.js";
export class SetEmail extends Command.Class<SetEmail>()("SetEmail", {
  payload: { email: EmailData },
  reply: Schema.Void,
}) {}
export type EmailCommand = SetEmail;
export const LarkEmailActor = ContextActor.define("lark/EmailActor", { commands: [SetEmail] })(
  Effect.gen(function* () {
    const session = yield* (yield* DurableContext)
      .open({
        path: yield* CurrentContextPath,
        state: EmailData,
        message: Schema.Never,
        serialize: JSON.stringify,
        events: "state",
      })
      .pipe(Effect.orDie);
    return {
      started: (context) => context.receiveTimeout("5 minutes"),
      receive: ({ email, replyTo }, context) => {
        if (contextPath(context) !== emailPath(email.mailbox, email.messageId))
          return Effect.die(new Error("Mail identity mismatch"));
        return session.state
          .set(email, { description: `An email in Lark mailbox ${email.mailbox}` })
          .pipe(Effect.orDie, Effect.andThen(replyTo.tell(undefined)));
      },
    };
  }),
);
