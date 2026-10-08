import { Effect, Schema } from "effect";
import { ContextQueryError } from "@aster/core";

export const AppName = Schema.Literals(["xiaohongshu", "ctrip"]);
export type AppName = typeof AppName.Type;
const text = Schema.NonEmptyString.check(
  Schema.isMaxLength(1000),
  Schema.isPattern(/^[^-]/),
  Schema.makeFilter((value) => !value.includes("\0")),
);
const id = Schema.String.check(Schema.isPattern(/^\d+$/));
const airport = Schema.String.check(Schema.isPattern(/^[A-Z]{3}$/));
const date = Schema.String.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}$/),
  Schema.makeFilter((value) => {
    const parsed = new Date(value);
    return !Number.isNaN(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
  }),
);
const limit = (max = 50) =>
  Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: max })));
const noteUrl = Schema.String.check(
  Schema.isMaxLength(4096),
  Schema.makeFilter((value) => {
    try {
      const url = new URL(value);
      return (
        url.protocol === "https:" &&
        ["www.xiaohongshu.com", "xiaohongshu.com"].includes(url.hostname) &&
        !url.username &&
        !url.password &&
        !url.port &&
        /^\/(explore|search_result)\/[a-zA-Z0-9]+$/.test(url.pathname) &&
        !!url.searchParams.get("xsec_token")
      );
    } catch {
      return false;
    }
  }),
);

type Args = Readonly<Record<string, string | number | boolean | undefined>>;
interface CommandSpec {
  readonly description: string;
  readonly schema: Schema.ConstraintDecoder<Args>;
  readonly positional: readonly string[];
  readonly flags?: Readonly<Record<string, string>>;
  readonly defaultLimit?: number;
  readonly dateRange?: readonly [string, string, boolean];
}
const search = (description: string): CommandSpec => ({
  description,
  schema: Schema.Struct({ query: text, limit: limit() }),
  positional: ["query"],
  flags: { limit: "limit" },
  defaultLimit: 10,
});
const route = (description: string, flight = false): CommandSpec => ({
  description,
  schema: Schema.Struct({
    from: flight ? airport : text,
    to: flight ? airport : text,
    date,
    limit: limit(),
  }),
  positional: ["from", "to"],
  flags: { date: "date", limit: "limit" },
  defaultLimit: 10,
});
const destination = (description: string, name: string): CommandSpec => ({
  description,
  schema: Schema.Struct({ [name]: text, limit: limit() }),
  positional: [name],
  flags: { limit: "limit" },
  defaultLimit: 10,
});

/** Explicit query allowlist, checked against OpenCLI 1.8.8. Never dispatch arbitrary adapter commands. */
export const appCommands: Record<AppName, Readonly<Record<string, CommandSpec>>> = {
  xiaohongshu: {
    search: {
      description:
        "Search travel experiences and recommendations. Preserve signed URLs for note/comments.",
      schema: Schema.Struct({
        query: text,
        limit: limit(),
        sort: Schema.optional(
          Schema.Literals([
            "comprehensive",
            "latest",
            "most-liked",
            "most-commented",
            "most-collected",
          ]),
        ),
        noteType: Schema.optional(Schema.Literals(["all", "video", "image"])),
        publishTime: Schema.optional(Schema.Literals(["anytime", "day", "week", "half-year"])),
      }),
      positional: ["query"],
      flags: { limit: "limit", sort: "sort", noteType: "note-type", publishTime: "publish-time" },
      defaultLimit: 10,
    },
    note: {
      description: "Read a full note using its signed Xiaohongshu URL with xsec_token from search.",
      schema: Schema.Struct({ url: noteUrl }),
      positional: ["url"],
    },
    comments: {
      description: "Read note comments; url must be a signed Xiaohongshu note URL with xsec_token.",
      schema: Schema.Struct({
        url: noteUrl,
        limit: limit(),
        withReplies: Schema.optional(Schema.Boolean),
      }),
      positional: ["url"],
      flags: { limit: "limit", withReplies: "with-replies" },
      defaultLimit: 10,
    },
    user: {
      description: "Read a user's public notes by profile ID.",
      schema: Schema.Struct({
        id: Schema.String.check(Schema.isPattern(/^[a-fA-F0-9]{24}$/)),
        limit: limit(),
      }),
      positional: ["id"],
      flags: { limit: "limit" },
      defaultLimit: 10,
    },
    feed: {
      description: "Read home feed recommendations.",
      schema: Schema.Struct({ limit: limit() }),
      positional: [],
      flags: { limit: "limit" },
      defaultLimit: 10,
    },
  },
  ctrip: {
    search: search("Find destinations, landmarks and city IDs for hotel/attraction queries."),
    "hotel-suggest": search("Find hotel names, IDs and city suggestions."),
    "hotel-search": {
      description:
        "List hotels for a numeric city ID and check-in/out dates; checkout must be after checkin.",
      schema: Schema.Struct({ city: id, checkin: date, checkout: date, limit: limit(30) }),
      positional: ["city"],
      flags: { checkin: "checkin", checkout: "checkout", limit: "limit" },
      defaultLimit: 10,
      dateRange: ["checkin", "checkout", false],
    },
    hotel: {
      description: "Read hotel details by numeric hotel ID.",
      schema: Schema.Struct({ id }),
      positional: ["id"],
    },
    attraction: {
      description: "List attractions for a numeric city ID.",
      schema: Schema.Struct({ city: id, limit: limit() }),
      positional: ["city"],
      flags: { limit: "limit" },
      defaultLimit: 10,
    },
    flight: route(
      "Find one-way flights using three-letter uppercase IATA codes and a departure date.",
      true,
    ),
    "flight-round": {
      description: "Find round-trip flights; return must be on or after depart.",
      schema: Schema.Struct({
        from: airport,
        to: airport,
        depart: date,
        return: date,
        limit: limit(),
      }),
      positional: ["from", "to"],
      flags: { depart: "depart", return: "return", limit: "limit" },
      defaultLimit: 10,
      dateRange: ["depart", "return", true],
    },
    train: route("Find trains by station/city names and departure date."),
    bus: route("Find intercity buses by city names and departure date."),
    ferry: route("Find ferries by city names and departure date."),
    cruise: destination("Find cruise packages from a departure port.", "port"),
    tour: destination("Find group/self-guided tours for a destination.", "destination"),
    package: destination("Find flight-plus-hotel packages for a destination.", "destination"),
  },
};

export const queryArgv = Effect.fn("Apps.queryArgv")(function* (
  app: AppName,
  command: string,
  raw: unknown,
) {
  const spec = Object.hasOwn(appCommands[app], command) ? appCommands[app][command] : undefined;
  if (!spec)
    return yield* new ContextQueryError({
      kind: "invalid-input",
      message: "Unsupported query command; read the Context commands catalogue",
    });
  const args = yield* Schema.decodeUnknownEffect(spec.schema, { onExcessProperty: "error" })(
    raw,
  ).pipe(
    Effect.mapError(
      () =>
        new ContextQueryError({
          kind: "invalid-input",
          message: "Invalid query arguments; check the Context commands catalogue",
        }),
    ),
  );
  if (spec.dateRange) {
    const [from, to, equal] = spec.dateRange;
    if (String(args[to]) < String(args[from]) || (!equal && args[to] === args[from]))
      return yield* new ContextQueryError({
        kind: "invalid-input",
        message: "Invalid query date range",
      });
  }
  const argv = [app, command, ...spec.positional.map((key) => String(args[key]))];
  for (const [key, flag] of Object.entries(spec.flags ?? {})) {
    const value = key === "limit" ? (args[key] ?? spec.defaultLimit) : args[key];
    if (value === true) argv.push(`--${flag}`);
    else if (value !== undefined && value !== false) argv.push(`--${flag}`, String(value));
  }
  return [...argv, "-f", "json"];
});
