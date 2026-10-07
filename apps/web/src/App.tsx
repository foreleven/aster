import React, { lazy, Suspense, useContext, useState, useSyncExternalStore } from "react";
import { RegistryContext, useAtomRefresh, useAtomSet, useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { QueryKeys } from "@aster/api";
import { connection } from "./api/events";
import { invalidateQueries } from "./api/client";
import { contextViews, dashboardStatus, selectedRow } from "./dashboard/state";
import { ContextNavigation } from "./contexts/navigation";
import { ContextWorkspace } from "./contexts/workspace";
import { GoalWorkspace } from "./goals/workspace";
import { EmptyState } from "./goals/presentation";
import "./goals/goals.css";
import "./contexts/contexts.css";

const subscribeSelection = (notify: () => void) => {
  window.addEventListener("popstate", notify);
  return () => window.removeEventListener("popstate", notify);
};
const readSelection = () => new URLSearchParams(window.location.search).get("context") ?? "";

const Inspector = lazy(() =>
  import("./dashboard/inspector").then((module) => ({ default: module.Inspector })),
);

export default function App() {
  const registry = useContext(RegistryContext);
  const contexts = useAtomValue(contextViews).filter((context) => !context.state.deleted);
  const { loaded, loading, connected, error } = useAtomValue(dashboardStatus);
  const goals = contexts.filter(
    (context) => /^\/goals\/[^/]+$/.test(context.path) && !context.state.deleted,
  );
  const selected = useSyncExternalStore(subscribeSelection, readSelection);
  const [inspected, setInspected] = useState("");
  const [actionError, setActionError] = useState("");
  const [mobileNavigation, setMobileNavigation] = useState(false);
  const active = selected
    ? contexts.find((context) => context.path === selected)
    : (contexts.find((context) => context.path === "/goals/personal") ?? goals[0] ?? contexts[0]);
  const navigate = (path: string) => {
    const url = new URL(window.location.href);
    url.searchParams.set("context", path);
    window.history.pushState(null, "", url);
    window.dispatchEvent(new PopStateEvent("popstate"));
    setMobileNavigation(false);
    setActionError("");
  };
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
      <ContextNavigation
        contexts={contexts}
        selected={active?.path ?? ""}
        connected={connected}
        loading={loading}
        refresh={refresh}
        close={() => setMobileNavigation(false)}
        onSelect={navigate}
      />
      {mobileNavigation && (
        <button
          className="navigation-backdrop"
          aria-label="Close context navigation"
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
            <EmptyState title={error ? "Could not load Contexts" : "Loading Contexts…"}>
              {error
                ? "Check the local connection, then retry."
                : "Connecting to your local workspace."}
            </EmptyState>
          </main>
        ) : active &&
          active.projection?.visibility !== "restricted" &&
          /^\/goals\/[^/]+$/.test(active.path) ? (
          <GoalWorkspace
            key={active.path}
            goal={active}
            contexts={contexts}
            inspect={inspect}
            showGoals={() => setMobileNavigation(true)}
          />
        ) : active ? (
          <ContextWorkspace
            key={active.path}
            context={active}
            contexts={contexts}
            navigate={navigate}
            showNavigation={() => setMobileNavigation(true)}
          />
        ) : (
          <main className="goals-empty-page">
            <button
              className="mobile-goals-toggle outline-action"
              onClick={() => setMobileNavigation(true)}
            >
              Choose context
            </button>
            <EmptyState title={selected ? "Context unavailable" : "No Contexts yet"}>
              {selected
                ? `The Context ${selected} is not available in the current snapshot. Select another Context or refresh.`
                : "Your workspace Contexts will appear here when the runtime is ready."}
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
