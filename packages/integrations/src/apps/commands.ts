import type { QueryCommand } from "@aster/core";
import { ContextCommand, ContextQueryError } from "@aster/core";
import { Effect, Schema } from "effect";

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
interface AppQueryCommand extends QueryCommand {
  readonly positional: readonly string[];
  readonly flags?: Readonly<Record<string, string>>;
  readonly defaultLimit?: number;
  readonly dateRange?: readonly [string, string, boolean];
  readonly payloadSchema: Schema.Codec<Args, unknown>;
}
/** Explicit query allowlist, checked against OpenCLI 1.8.8. Never dispatch arbitrary adapter commands. */
export class XiaohongshuSearch extends ContextCommand.Class<XiaohongshuSearch>()("search", {
  success: Schema.Json,
  error: ContextQueryError,
  description:
    "Search travel experiences and recommendations. Preserve signed URLs for note/comments.",
  payload: Schema.Struct({
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
  }).fields,
}) {
  static readonly positional = ["query"] as const;
  static readonly flags = {
    limit: "limit",
    sort: "sort",
    noteType: "note-type",
    publishTime: "publish-time",
  } as const;
  static readonly defaultLimit = 10 as const;
}
export class XiaohongshuNote extends ContextCommand.Class<XiaohongshuNote>()("note", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Read a full note using its signed Xiaohongshu URL with xsec_token from search.",
  payload: Schema.Struct({ url: noteUrl }).fields,
}) {
  static readonly positional = ["url"] as const;
}
export class XiaohongshuComments extends ContextCommand.Class<XiaohongshuComments>()("comments", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Read note comments; url must be a signed Xiaohongshu note URL with xsec_token.",
  payload: Schema.Struct({
    url: noteUrl,
    limit: limit(),
    withReplies: Schema.optional(Schema.Boolean),
  }).fields,
}) {
  static readonly positional = ["url"] as const;
  static readonly flags = { limit: "limit", withReplies: "with-replies" } as const;
  static readonly defaultLimit = 10 as const;
}
export class XiaohongshuUser extends ContextCommand.Class<XiaohongshuUser>()("user", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Read a user's public notes by profile ID.",
  payload: Schema.Struct({
    id: Schema.String.check(Schema.isPattern(/^[a-fA-F0-9]{24}$/)),
    limit: limit(),
  }).fields,
}) {
  static readonly positional = ["id"] as const;
  static readonly flags = { limit: "limit" } as const;
  static readonly defaultLimit = 10 as const;
}
export class XiaohongshuFeed extends ContextCommand.Class<XiaohongshuFeed>()("feed", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Read home feed recommendations.",
  payload: Schema.Struct({ limit: limit() }).fields,
}) {
  static readonly positional = [] as const;
  static readonly flags = { limit: "limit" } as const;
  static readonly defaultLimit = 10 as const;
}
export class CtripHotelSearch extends ContextCommand.Class<CtripHotelSearch>()("hotel-search", {
  success: Schema.Json,
  error: ContextQueryError,
  description:
    "List hotels for a numeric city ID and check-in/out dates; checkout must be after checkin.",
  payload: Schema.Struct({ city: id, checkin: date, checkout: date, limit: limit(30) }).fields,
}) {
  static readonly positional = ["city"] as const;
  static readonly flags = { checkin: "checkin", checkout: "checkout", limit: "limit" } as const;
  static readonly defaultLimit = 10 as const;
  static readonly dateRange = ["checkin", "checkout", false] as const;
}
export class CtripHotel extends ContextCommand.Class<CtripHotel>()("hotel", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Read hotel details by numeric hotel ID.",
  payload: Schema.Struct({ id }).fields,
}) {
  static readonly positional = ["id"] as const;
}
export class CtripAttraction extends ContextCommand.Class<CtripAttraction>()("attraction", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "List attractions for a numeric city ID.",
  payload: Schema.Struct({ city: id, limit: limit() }).fields,
}) {
  static readonly positional = ["city"] as const;
  static readonly flags = { limit: "limit" } as const;
  static readonly defaultLimit = 10 as const;
}
export class CtripFlightRound extends ContextCommand.Class<CtripFlightRound>()("flight-round", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Find round-trip flights; return must be on or after depart.",
  payload: Schema.Struct({
    from: airport,
    to: airport,
    depart: date,
    return: date,
    limit: limit(),
  }).fields,
}) {
  static readonly positional = ["from", "to"] as const;
  static readonly flags = { depart: "depart", return: "return", limit: "limit" } as const;
  static readonly defaultLimit = 10 as const;
  static readonly dateRange = ["depart", "return", true] as const;
}
export class CtripSearch extends ContextCommand.Class<CtripSearch>()("search", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Find destinations, landmarks and city IDs for hotel/attraction queries.",
  payload: { query: text, limit: limit() },
}) {
  static readonly positional = ["query"] as const;
  static readonly flags = { limit: "limit" };
  static readonly defaultLimit = 10;
}
export class CtripHotelSuggest extends ContextCommand.Class<CtripHotelSuggest>()("hotel-suggest", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Find hotel names, IDs and city suggestions.",
  payload: { query: text, limit: limit() },
}) {
  static readonly positional = ["query"] as const;
  static readonly flags = { limit: "limit" };
  static readonly defaultLimit = 10;
}
export class CtripFlight extends ContextCommand.Class<CtripFlight>()("flight", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Find one-way flights using three-letter uppercase IATA codes and a departure date.",
  payload: { from: airport, to: airport, date, limit: limit() },
}) {
  static readonly positional = ["from", "to"] as const;
  static readonly flags = { date: "date", limit: "limit" };
  static readonly defaultLimit = 10;
}
export class CtripTrain extends ContextCommand.Class<CtripTrain>()("train", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Find trains by station/city names and departure date.",
  payload: { from: text, to: text, date, limit: limit() },
}) {
  static readonly positional = ["from", "to"] as const;
  static readonly flags = { date: "date", limit: "limit" };
  static readonly defaultLimit = 10;
}
export class CtripBus extends ContextCommand.Class<CtripBus>()("bus", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Find intercity buses by city names and departure date.",
  payload: { from: text, to: text, date, limit: limit() },
}) {
  static readonly positional = ["from", "to"] as const;
  static readonly flags = { date: "date", limit: "limit" };
  static readonly defaultLimit = 10;
}
export class CtripFerry extends ContextCommand.Class<CtripFerry>()("ferry", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Find ferries by city names and departure date.",
  payload: { from: text, to: text, date, limit: limit() },
}) {
  static readonly positional = ["from", "to"] as const;
  static readonly flags = { date: "date", limit: "limit" };
  static readonly defaultLimit = 10;
}
export class CtripCruise extends ContextCommand.Class<CtripCruise>()("cruise", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Find cruise packages from a departure port.",
  payload: { port: text, limit: limit() },
}) {
  static readonly positional = ["port"] as const;
  static readonly flags = { limit: "limit" };
  static readonly defaultLimit = 10;
}
export class CtripTour extends ContextCommand.Class<CtripTour>()("tour", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Find group/self-guided tours for a destination.",
  payload: { destination: text, limit: limit() },
}) {
  static readonly positional = ["destination"] as const;
  static readonly flags = { limit: "limit" };
  static readonly defaultLimit = 10;
}
export class CtripPackage extends ContextCommand.Class<CtripPackage>()("package", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Find flight-plus-hotel packages for a destination.",
  payload: { destination: text, limit: limit() },
}) {
  static readonly positional = ["destination"] as const;
  static readonly flags = { limit: "limit" };
  static readonly defaultLimit = 10;
}
export const appCommands = {
  xiaohongshu: [
    XiaohongshuSearch,
    XiaohongshuNote,
    XiaohongshuComments,
    XiaohongshuUser,
    XiaohongshuFeed,
  ],
  ctrip: [
    CtripSearch,
    CtripHotelSuggest,
    CtripHotelSearch,
    CtripHotel,
    CtripAttraction,
    CtripFlight,
    CtripFlightRound,
    CtripTrain,
    CtripBus,
    CtripFerry,
    CtripCruise,
    CtripTour,
    CtripPackage,
  ],
} as const satisfies Record<AppName, readonly AppQueryCommand[]>;

export const queryArgv = Effect.fn("Apps.queryArgv")(function* (
  app: AppName,
  command: string,
  raw: unknown,
) {
  const spec: AppQueryCommand | undefined = appCommands[app].find(
    (candidate) => candidate._tag === command,
  );
  if (!spec)
    return yield* new ContextQueryError({
      kind: "invalid-input",
      message: "Unsupported query command; read the Context commands catalogue",
    });
  const args = yield* Schema.decodeUnknownEffect(spec.payloadSchema, { onExcessProperty: "error" })(
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
