import { DateTime, Option, Schema } from "effect";

export const UserDisplayName = Schema.String.check(
  Schema.makeFilter((value) => value.trim().length > 0, {
    expected: "A non-blank string",
  }),
);

/** User-stated response preferences that should remain stable outside Memory. */
export const UserProfilePreferences = Schema.Struct({
  responseStyle: Schema.Literals(["concise", "balanced", "detailed"]),
});
export type UserProfilePreferences = typeof UserProfilePreferences.Type;

/** The small durable user model used by core workflows. Other personal facts belong in Memory. */
export const UserProfile = Schema.Struct({
  displayName: Schema.optional(UserDisplayName),
  timeZone: Schema.String.check(
    Schema.makeFilter((value) => Option.isSome(DateTime.zoneMakeNamed(value)), {
      expected: "An IANA time zone",
    }),
  ),
  preferences: UserProfilePreferences,
});
export type UserProfile = typeof UserProfile.Type;

export const defaultUserProfile = (): UserProfile => ({
  timeZone: "Asia/Shanghai",
  preferences: { responseStyle: "balanced" },
});

export const serializeUserProfile = (profile: Readonly<UserProfile>): string => {
  const lines = [
    "## User profile",
    `Time zone: ${profile.timeZone}`,
    `Response style: ${profile.preferences.responseStyle}`,
  ];
  if (profile.displayName) lines.splice(1, 0, `Display name: ${profile.displayName}`);
  return lines.join("\n");
};
