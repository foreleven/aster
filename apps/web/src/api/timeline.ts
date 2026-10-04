import { Effect } from "effect";
import { Atom } from "effect/reactivity";
import {
  QueryKeys,
  type RetryGoalTurnInput,
  type RetryGoalSignalInput,
} from "@aster/api-contracts";
import { ApplicationClient } from "./client";

export const goalTimeline = Atom.family((slug: string) => {
  const before = Atom.make<number | undefined>(undefined);
  const latest = ApplicationClient.query(
    "GetGoalTimeline",
    { slug },
    {
      reactivityKeys: [QueryKeys.all, QueryKeys.history(slug)],
    },
  );
  const feed = ApplicationClient.runtime.atom((get) => {
    const requested = get(before);
    return Effect.gen(function* () {
      const newest = yield* get.result(latest, { suspendOnWaiting: true });
      const client = yield* ApplicationClient;
      let page = newest;
      let groups = [...newest.groups];
      // Evaluation statuses are mutable. Re-read the whole loaded range after invalidation;
      // never retain an old completed/unknown decision from an immutable-history cache.
      while (requested !== undefined && page.nextBefore !== null && page.nextBefore >= requested) {
        page = yield* client("GetGoalTimeline", { slug, before: page.nextBefore });
        groups = [...page.groups, ...groups];
      }
      return { ...newest, groups, nextBefore: page.nextBefore };
    });
  });
  return { before, latest, feed };
});

// Preserve an uncertain user authorization while navigating or refreshing live data.
export const pendingSignalRetries = Atom.make<Record<string, RetryGoalSignalInput>>({}).pipe(
  Atom.keepAlive,
);

export const pendingTurnRetries = Atom.make<Record<string, RetryGoalTurnInput>>({}).pipe(
  Atom.keepAlive,
);
