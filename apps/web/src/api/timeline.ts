import { Effect, Option } from "effect";
import { AsyncResult, Atom } from "effect/reactivity";
import { QueryKeys, type GoalTimelinePage, type RetryGoalTurnInput } from "@aster/api-contracts";
import { ApplicationClient } from "./client";

type TimelineFeed = GoalTimelinePage & { readonly loadedBefore?: number };

const mergeMessages = (
  ...pages: ReadonlyArray<GoalTimelinePage["messages"]>
): GoalTimelinePage["messages"] =>
  [
    ...new Map(
      pages.flatMap((messages) => messages.map((entry) => [entry.id, entry] as const)),
    ).values(),
  ].sort((a, b) => a.id - b.id);

/** The public conversation is append-only. Retain committed pages; interrupted refreshes never install partial cache results. */
const readFeed = Effect.fn("TimelineFeed.read")(function* (
  slug: string,
  requestedBefore: number | undefined,
  cached: TimelineFeed | undefined,
  newest: GoalTimelinePage,
) {
  const client = yield* ApplicationClient;
  // A reset/truncated store invalidates the cache. Ordinary commits only append immutable messages.
  const retained = cached && newest.total >= cached.total ? cached : undefined;
  const boundary = retained?.messages.at(-1)?.id;
  let page = newest;
  const pages = [page.messages];
  while (boundary !== undefined && page.nextBefore !== null && page.messages[0]?.id > boundary) {
    page = yield* client("GetGoalTimeline", { slug, before: page.nextBefore });
    pages.push(page.messages);
  }
  const additions = pages.flat().filter((entry) => boundary === undefined || entry.id > boundary);
  let messages = mergeMessages(retained?.messages ?? [], additions);
  let nextBefore = retained?.messages.length ? retained.nextBefore : page.nextBefore;
  if (retained && requestedBefore !== undefined && requestedBefore !== retained.loadedBefore) {
    const older = yield* client("GetGoalTimeline", { slug, before: requestedBefore });
    messages = mergeMessages(older.messages, messages);
    nextBefore = older.nextBefore;
  }
  return { messages, total: newest.total, nextBefore, loadedBefore: requestedBefore };
});

export const goalTimeline = Atom.family((slug: string) => {
  const before = Atom.make<number | undefined>(undefined);
  // Tail invalidation and backward pagination are independent dependencies. Loading an older page
  // reuses this query; an SSE update during that load restarts the merge with the fresh tail.
  const latest = ApplicationClient.query(
    "GetGoalTimeline",
    { slug },
    {
      reactivityKeys: [QueryKeys.all, QueryKeys.history(slug)],
    },
  );
  const feed = ApplicationClient.runtime.atom((get) => {
    const cached = get
      .self<AsyncResult.AsyncResult<TimelineFeed, unknown>>()
      .pipe(Option.flatMap(AsyncResult.value), Option.getOrUndefined);
    const requested = get(before);
    return get
      .result(latest, { suspendOnWaiting: true })
      .pipe(Effect.flatMap((newest) => readFeed(slug, requested, cached, newest)));
  });
  return { before, feed, latest };
});

// Preserve an uncertain user authorization while navigating or refreshing live data.
export const pendingTurnRetries = Atom.make<Record<string, RetryGoalTurnInput>>({}).pipe(
  Atom.keepAlive,
);
