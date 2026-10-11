import { Command } from "@aster/actor";
import { Effect, Match, Schema } from "effect";
import { ContextActor } from "../context/actor.js";
import { CurrentContextPath } from "../context/location.js";
import { DurableContext } from "../context/store.js";
import { ApplicationError } from "../operations.js";
import { ContextQueryError } from "../queries/contracts.js";
import {
  defaultUserProfile,
  serializeUserProfile,
  UserProfile,
  UserDisplayName,
  UserProfilePreferences,
} from "./model.js";
import { ReadUserProfile } from "./queries.js";

const UserProfileUpdate = Schema.Struct({
  displayName: Schema.optional(Schema.NullOr(UserDisplayName)),
  timeZone: Schema.optional(UserProfile.fields.timeZone),
  responseStyle: Schema.optional(UserProfilePreferences.fields.responseStyle),
});
export type UserProfileUpdate = typeof UserProfileUpdate.Type;

export class UpdateUserProfile extends Command.Class<UpdateUserProfile>()("UpdateProfile", {
  description: "Update the user's explicit profile fields.",
  payload: { update: UserProfileUpdate },
  success: UserProfile,
  error: ApplicationError,
}) {}

export const UserProfileActor = ContextActor.define("profile/Actor", {
  commands: [ReadUserProfile, UpdateUserProfile],
})(
  Effect.gen(function* () {
    const session = yield* (yield* DurableContext)
      .open({
        path: yield* CurrentContextPath,
        state: UserProfile,
        message: Schema.Never,
        serialize: serializeUserProfile,
        initial: {
          description: "User profile and explicit response preferences",
          state: defaultUserProfile(),
        },
        events: "none",
      })
      .pipe(Effect.orDie);

    const read = session.state.get.pipe(
      Effect.mapError(
        () =>
          new ContextQueryError({
            kind: "unavailable",
            message: "User profile is unavailable",
          }),
      ),
    );
    const update = Effect.fn("UserProfile.update")(function* (patch: UserProfileUpdate) {
      return yield* session.state
        .update((profile) => {
          const withDisplayName = Match.value(patch.displayName).pipe(
            Match.when(null, () => {
              const { displayName: _displayName, ...withoutDisplayName } = profile;
              return withoutDisplayName;
            }),
            Match.when(undefined, () => profile),
            Match.orElse((displayName) => ({ ...profile, displayName })),
          );
          const withTimeZone =
            patch.timeZone === undefined
              ? withDisplayName
              : { ...withDisplayName, timeZone: patch.timeZone };
          return patch.responseStyle === undefined
            ? withTimeZone
            : {
                ...withTimeZone,
                preferences: { ...profile.preferences, responseStyle: patch.responseStyle },
              };
        })
        .pipe(
          Effect.map(({ state }) => state),
          Effect.mapError(
            () =>
              new ApplicationError({
                kind: "unavailable",
                message: "User profile could not be saved",
              }),
          ),
        );
    });

    return {
      receive: (command, _context) =>
        Match.value(command).pipe(
          Match.tag("profile", ({ replyTo }) => Command.reply(replyTo, read)),
          Match.tag("UpdateProfile", ({ update: patch, replyTo }) =>
            Command.reply(replyTo, update(patch)),
          ),
          Match.exhaustive,
        ),
    };
  }),
);
