import React, { useMemo } from "react";
import { useAtomSet, useAtomValue, useAtomRefresh } from "@effect/atom-react";
import { resultError, resultValue } from "../api/client";
import { goalHistory } from "../api/history";
import { Button } from "@/components/ui/button";
import { Messages } from "./shared";

import { projectMessage } from "./model";

export function GoalFeed({ slug, inspect }: { slug: string; inspect: (path: string) => void }) {
  // Retain the family bundle: Atom.family uses weak references, while hooks only retain its individual atoms.
  const atoms = useMemo(() => goalHistory(slug), [slug]);
  const result = useAtomValue(atoms.feed);
  const setBefore = useAtomSet(atoms.before);
  const retryFeed = useAtomRefresh(atoms.feed);
  const refreshTail = useAtomRefresh(atoms.latest);
  const retry = () => {
    refreshTail();
    retryFeed();
  };
  const page = resultValue(result);
  const error = resultError(result);
  return (
    <div>
      {error && (
        <p role="alert" className="text-destructive my-3">
          {error}{" "}
          <Button onClick={retry} variant="outline">
            Retry history
          </Button>
        </p>
      )}
      {page?.nextBefore && (
        <Button
          variant="outline"
          className="my-3"
          disabled={result.waiting}
          onClick={() => setBefore(page.nextBefore ?? undefined)}
        >
          {result.waiting ? "Loading…" : "Load earlier records"}
        </Button>
      )}
      <Messages
        messages={(page?.entries ?? []).map((entry) => ({
          ...projectMessage(entry.message),
          at: entry.at,
        }))}
        inspect={inspect}
      />
    </div>
  );
}
