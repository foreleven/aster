import { Schema } from "effect";
import { DEFAULT_TIME_ZONE } from "../service/dates.js";

export const RetrievalProgressSchema = Schema.Struct({
  version: Schema.Literal(1),
  date: Schema.String,
  timeZone: Schema.Literal(DEFAULT_TIME_ZONE),
  intervals: Schema.Array(Schema.Struct({ from: Schema.String, through: Schema.String })).pipe(
    Schema.mutable,
  ),
  through: Schema.optional(Schema.String),
});
export type RetrievalProgress = typeof RetrievalProgressSchema.Type;

/** Merge overlaps while retaining gaps; through remains the latest retrieved endpoint. */
export const extendRetrieval = (
  day: RetrievalProgress,
  start: number,
  end: number,
): RetrievalProgress => {
  const intervals: Array<[number, number]> = [
    ...day.intervals.map((value): [number, number] => [
      Date.parse(value.from),
      Date.parse(value.through),
    ]),
    [start, end],
  ];
  intervals.sort((a, b) => a[0] - b[0]);
  const merged: Array<[number, number]> = [];
  for (const interval of intervals) {
    const last = merged.at(-1);
    if (last && interval[0] <= last[1]) last[1] = Math.max(last[1], interval[1]);
    else merged.push([...interval]);
  }
  const next = merged.map(([from, through]) => ({
    from: new Date(from).toISOString(),
    through: new Date(through).toISOString(),
  }));
  return { ...day, intervals: next, through: next.at(-1)!.through };
};
