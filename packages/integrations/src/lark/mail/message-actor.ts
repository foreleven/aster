import { Command as ActorCommand } from "@aster/actor";
import { Effect, Match, Schema } from "effect";

import { ContextActor, ContextQueryError, DurableContext, contextPath } from "@aster/core";
import { EmailData } from "./model.js";
export class SetEmail extends ActorCommand.Class<SetEmail>()("SetEmail", {
  payload: { email: EmailData },
  reply: Schema.Void,
}) {}

export class GetEmail extends ActorCommand.Class<GetEmail>()("GetEmail", {
  payload: {},
  success: EmailData,
  error: ContextQueryError,
}) {}
export type MailMessageCommand = SetEmail | GetEmail;

export const LarkMailMessageActor = ContextActor.define("lark/MailMessageActor", {
  commands: [SetEmail, GetEmail],
})((owner) =>
  Effect.gen(function* () {
    const session = yield* (yield* DurableContext)
      .open({
        path: contextPath(owner),
        state: EmailData,
        message: Schema.Never,
        serialize: (state) => JSON.stringify(state),
        events: "state",
      })
      .pipe(Effect.orDie);
    return {
      started: (context) => context.receiveTimeout("5 minutes"),
      receive: (command, context) =>
        Match.value(command).pipe(
          Match.tag("GetEmail", ({ replyTo }) =>
            ActorCommand.reply(
              replyTo,
              Effect.gen(function* () {
                const record = yield* session.current.pipe(Effect.orDie);
                if (!record)
                  return yield* new ContextQueryError({
                    kind: "unavailable",
                    message: "Email evidence unavailable",
                  });
                return yield* Schema.decodeUnknownEffect(EmailData)(record.state).pipe(
                  Effect.orDie,
                );
              }),
            ),
          ),
          Match.tag("SetEmail", ({ email, replyTo }) => {
            const path = contextPath(context);
            if (path !== `/lark/mail/${email.mailbox}/${email.messageId}`)
              return Effect.die(new Error("Mail identity mismatch"));
            return session.state
              .set(email, { ...{}, description: `An email in Lark mailbox ${email.mailbox}` })
              .pipe(Effect.orDie, Effect.andThen(replyTo.tell(undefined)));
          }),
          Match.exhaustive,
        ),
    };
  }),
);
