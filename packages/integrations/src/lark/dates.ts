import { DateTime, Option, Schema } from "effect";

export const DEFAULT_TIME_ZONE = "Asia/Shanghai";
/** Persisted instants are validated before calendar operations. */
export const Timestamp = Schema.String.check(
  Schema.makeFilter(
    (value) => /T.*(?:Z|[+-]\d{2}:\d{2})$/.test(value) && Option.isSome(DateTime.make(value)),
  ),
);
export const CalendarDate = Schema.String.check(
  Schema.makeFilter((value) => {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
    const parsed = DateTime.make(`${value}T00:00:00+08:00`);
    return (
      Option.isSome(parsed) &&
      DateTime.formatIsoDate(DateTime.setZoneNamedUnsafe(parsed.value, DEFAULT_TIME_ZONE)) === value
    );
  }),
);
/** These pure helpers accept validated domain timestamps/dates and Clock milliseconds. */
export const formatDate = (time: string | number) =>
  DateTime.makeUnsafe(time).pipe(
    DateTime.setZoneNamedUnsafe(DEFAULT_TIME_ZONE),
    DateTime.formatIsoDate,
  );
export const dayStart = (date: string) =>
  DateTime.makeUnsafe(`${date}T00:00:00+08:00`).pipe(DateTime.toEpochMillis);
export const nextDay = (date: string) => formatDate(dayStart(date) + 86_400_000);
