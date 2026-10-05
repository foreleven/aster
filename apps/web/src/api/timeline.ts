import { Effect } from "effect";
import { Atom } from "effect/reactivity";
import { QueryKeys, type RetryGoalTurnInput } from "@aster/api-contracts";
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
      let messages = [...newest.messages];
      // Re-read the loaded range on reconnect so newly committed replies are included.
      while (requested !== undefined && page.nextBefore !== null && page.nextBefore >= requested) {
        page = yield* client("GetGoalTimeline", { slug, before: page.nextBefore });
        messages = [...page.messages, ...messages];
      }
      return { ...newest, messages, nextBefore: page.nextBefore };
    });
  });
  return { before, latest, feed };
});

// Preserve an uncertain user authorization while navigating or refreshing live data.
export const pendingTurnRetries = Atom.make<Record<string, RetryGoalTurnInput>>({}).pipe(
  Atom.keepAlive,
);
