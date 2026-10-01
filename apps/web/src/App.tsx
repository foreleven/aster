import React, { lazy, Suspense, useContext, useState } from "react";
import { RegistryContext, useAtomRefresh, useAtomSet, useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/unstable/reactivity";
import { QueryKeys } from "@aster/api-contracts";
import { connection } from "./api/events";
import { invalidateQueries } from "./api/client";
import { contextViews, dashboardStatus, selectedRow } from "./dashboard/state";
import { GoalsNavigation } from "./goals/navigation";
import { GoalWorkspace } from "./goals/workspace";
import { EmptyState } from "./goals/presentation";
import "./goals/goals.css";

const Inspector = lazy(() =>
  import("./dashboard/inspector").then((module) => ({ default: module.Inspector })),
);

export default function App() {
  const registry = useContext(RegistryContext);
  const contexts = useAtomValue(contextViews);
  const { loaded, loading, connected, error } = useAtomValue(dashboardStatus);
  const goals = contexts.filter(
    (context) => /^\/goals\/[^/]+$/.test(context.path) && !context.state.deleted,
  );
  const [selected, setSelected] = useState("");
  const [inspected, setInspected] = useState("");
  const [actionError, setActionError] = useState("");
  const [mobileNavigation, setMobileNavigation] = useState(false);
  const active = goals.find((goal) => goal.path === selected) ?? goals[0];
  const invalidate = useAtomSet(invalidateQueries);
  const reconnect = useAtomRefresh(connection);
  const refresh = () => {
    setActionError("");
    invalidate([QueryKeys.all]);
    if (AsyncResult.isFailure(registry.get(connection))) reconnect();
  };
  const inspect = (path: string) => {
    if (!registry.get(selectedRow(path))) {
      setActionError(`Context not available: ${path}`);
      return;
    }
    setInspected(path);
  };
  return (
    <div className={`goals-app ${mobileNavigation ? "mobile-navigation-open" : ""}`}>
      <GoalsNavigation
        goals={goals}
        selected={active?.path ?? ""}
        connected={connected}
        loading={loading}
        refresh={refresh}
        close={() => setMobileNavigation(false)}
        onSelect={(path) => {
          setSelected(path);
          setMobileNavigation(false);
          setActionError("");
        }}
      />
      {mobileNavigation && (
        <button
          className="navigation-backdrop"
          aria-label="Close goal navigation"
          onClick={() => setMobileNavigation(false)}
        />
      )}
      <div className="goal-content-shell">
        {(error || actionError) && (
          <div role="alert" className="goals-error">
            {error || actionError}
            <button onClick={refresh}>Retry</button>
          </div>
        )}
        {!loaded ? (
          <main className="goals-empty-page">
            <EmptyState title={error ? "Could not load Goals" : "Loading Goals…"}>
              {error
                ? "Check the local connection, then retry."
                : "Connecting to your local workspace."}
            </EmptyState>
          </main>
        ) : active ? (
          <GoalWorkspace
            key={active.path}
            goal={active}
            contexts={contexts}
            inspect={inspect}
            showGoals={() => setMobileNavigation(true)}
          />
        ) : (
          <main className="goals-empty-page">
            <EmptyState title="No Goals yet">
              Add a goal to your Aster configuration to start tracking progress, context, and
              actions.
            </EmptyState>
          </main>
        )}
      </div>
      {inspected && (
        <Suspense
          fallback={
            <div className="inspector-loading" role="status">
              Loading context…
            </div>
          }
        >
          <Inspector
            key={inspected}
            path={inspected}
            close={() => setInspected("")}
            inspect={inspect}
            report={setActionError}
          />
        </Suspense>
      )}
    </div>
  );
}
