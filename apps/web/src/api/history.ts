import { Effect, Option } from "effect";
import { AsyncResult, Atom } from "effect/reactivity";
import { QueryKeys, type HistoryPage } from "@aster/api-contracts";
import { ApplicationClient } from "./client";

type HistoryFeed = HistoryPage & { readonly loadedBefore?: number };

const mergeEntries = (...pages: ReadonlyArray<HistoryPage["entries"]>): HistoryPage["entries"] =>
  [
    ...new Map(
      pages.flatMap((entries) => entries.map((entry) => [entry.seq, entry] as const)),
    ).values(),
  ].sort((a, b) => a.seq - b.seq);

/** History is append-only. Retain committed pages; interrupted refreshes never install partial cache results. */
const readFeed = Effect.fn("HistoryFeed.read")(function* (
  slug: string,
  requestedBefore: number | undefined,
  cached: HistoryFeed | undefined,
  newest: HistoryPage,
) {
  const client = yield* ApplicationClient;
  // A reset/truncated store invalidates the cache. Ordinary commits only append immutable entries.
  const retained = cached && newest.total >= cached.total ? cached : undefined;
  const boundary = retained?.entries.at(-1)?.seq;
  let page = newest;
  const pages = [page.entries];
  while (boundary !== undefined && page.nextBefore !== null && page.entries[0]?.seq > boundary) {
    page = yield* client("GetGoalHistory", { slug, before: page.nextBefore });
    pages.push(page.entries);
  }
  const additions = pages.flat().filter((entry) => boundary === undefined || entry.seq > boundary);
  let entries = mergeEntries(retained?.entries ?? [], additions);
  let nextBefore = retained?.entries.length ? retained.nextBefore : page.nextBefore;
  if (retained && requestedBefore !== undefined && requestedBefore !== retained.loadedBefore) {
    const older = yield* client("GetGoalHistory", { slug, before: requestedBefore });
    entries = mergeEntries(older.entries, entries);
    nextBefore = older.nextBefore;
  }
  return { entries, total: newest.total, nextBefore, loadedBefore: requestedBefore };
});

export const goalHistory = Atom.family((slug: string) => {
  const before = Atom.make<number | undefined>(undefined);
  // Tail invalidation and backward pagination are independent dependencies. Loading an older page
  // reuses this query; an SSE update during that load restarts the merge with the fresh tail.
  const latest = ApplicationClient.query(
    "GetGoalHistory",
    { slug },
    {
      reactivityKeys: [QueryKeys.all, QueryKeys.history(slug)],
    },
  );
  const feed = ApplicationClient.runtime.atom((get) => {
    const cached = get
      .self<AsyncResult.AsyncResult<HistoryFeed, unknown>>()
      .pipe(Option.flatMap(AsyncResult.value), Option.getOrUndefined);
    const requested = get(before);
    return get
      .result(latest, { suspendOnWaiting: true })
      .pipe(Effect.flatMap((newest) => readFeed(slug, requested, cached, newest)));
  });
  return { before, feed, latest };
});
