import { Schema } from "effect";

export const QueryKeys = {
  all: "all-queries",
  contexts: "contexts",
  goals: "goals",
  approvals: "approvals",
  runtime: "runtime",
  context: (path: string) => `context:${path}`,
  history: (slug: string) => `goal-history:${slug}`,
} as const;

/** Stable wire keys shared by server commit notifications and client mutation invalidation. */
export const contextQueryKeys = (path: string): readonly string[] => {
  const keys = [QueryKeys.contexts, QueryKeys.context(path)];
  const goal = /^\/goals\/([^/]+)$/.exec(path);
  if (goal) keys.push(QueryKeys.goals, QueryKeys.history(goal[1]!));
  if (path === "/approvals") keys.push(QueryKeys.approvals);
  return keys;
};
export const QueryInvalidation = Schema.TaggedStruct("Invalidate", {
  keys: Schema.Array(Schema.String),
});
export type QueryInvalidation = typeof QueryInvalidation.Type;
