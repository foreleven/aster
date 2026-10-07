import { useContext, useState, useSyncExternalStore } from "react";
import { RegistryContext, useAtomRefresh, useAtomSet, useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { QueryKeys } from "@aster/api";
import { connection } from "./api/events";
import { invalidateQueries } from "./api/client";
import { contextViews, connectionStatus } from "./api/state";
import { SidebarProvider, SidebarInset, SidebarTrigger } from "./components/ui/sidebar";
import { Separator } from "./components/ui/separator";
import { TooltipProvider } from "./components/ui/tooltip";
import { ContextNavigation, type WorkspaceView } from "./contexts/navigation";
import { ContextWorkspace } from "./contexts/workspace";
import { GoalWorkspace } from "./goals/workspace";
import { Collection } from "./assistant/collection";
import { Approvals } from "./approvals/panel";
import { EmptyState, ErrorNotice, Loading } from "./components/feedback";
import { kindOf } from "./contexts/model";

const subscribeSelection = (notify: () => void) => {
  window.addEventListener("popstate", notify);
  return () => window.removeEventListener("popstate", notify);
};
const selection = () => window.location.search;
export default function App() {
  const registry = useContext(RegistryContext);
  const contexts = useAtomValue(contextViews);
  const status = useAtomValue(connectionStatus);
  const search = useSyncExternalStore(subscribeSelection, selection);
  const params = new URLSearchParams(search);
  const path = params.get("context");
  const requested = params.get("view");
  const view =
    requested === "tasks" ||
    requested === "following" ||
    requested === "sources" ||
    requested === "approvals"
      ? requested
      : "";
  const active = path
    ? contexts.find((context) => context.path === path)
    : contexts.find((context) => context.path === "/goals/personal");
  const [actionError, setActionError] = useState("");
  const invalidate = useAtomSet(invalidateQueries);
  const reconnect = useAtomRefresh(connection);
  const refresh = () => {
    invalidate([QueryKeys.all]);
    if (AsyncResult.isFailure(registry.get(connection))) reconnect();
  };
  const select = (path?: string, view?: WorkspaceView) => {
    const url = new URL(window.location.href);
    url.searchParams.delete("view");
    url.searchParams.delete("context");
    if (path) url.searchParams.set("context", path);
    if (view) url.searchParams.set("view", view);
    window.history.pushState(null, "", url);
    window.dispatchEvent(new PopStateEvent("popstate"));
    setActionError("");
  };
  const navigate = (path: string) => select(path);
  return (
    <TooltipProvider>
      <SidebarProvider className="h-dvh min-h-0">
        <ContextNavigation
          contexts={contexts}
          selected={active?.path ?? ""}
          view={view}
          navigate={navigate}
          selectView={(view) => select(undefined, view)}
          connected={status.connected}
          loading={status.loading}
          refresh={refresh}
        />
        <SidebarInset className="min-w-0 overflow-hidden">
          <header className="flex h-14 shrink-0 items-center gap-3 border-b px-5">
            <SidebarTrigger />
            <Separator orientation="vertical" className="h-4" />
            <span className="text-xs text-muted-foreground">Your personal assistant</span>
          </header>
          {status.error && (
            <div className="px-5 pt-4">
              <ErrorNotice error={status.error} retry={refresh} />
            </div>
          )}
          <div className="flex min-h-0 flex-1 flex-col overflow-y-auto">
            {!status.loaded ? (
              <Loading />
            ) : view === "approvals" ? (
              <section className="mx-auto flex w-full max-w-3xl flex-col gap-6 p-5 md:p-10">
                <header>
                  <h1 className="text-3xl font-medium tracking-tight">Needs you</h1>
                  <p className="mt-2 text-muted-foreground">
                    Decisions and questions waiting for your response.
                  </p>
                </header>
                <ErrorNotice error={actionError} />
                <Approvals inspect={navigate} report={setActionError} />
              </section>
            ) : view ? (
              <Collection key={view} view={view} contexts={contexts} navigate={navigate} />
            ) : active ? (
              kindOf(active.path) === "goal" && active.projection?.visibility !== "restricted" ? (
                <GoalWorkspace
                  key={active.path}
                  goal={active}
                  contexts={contexts}
                  navigate={navigate}
                />
              ) : (
                <ContextWorkspace
                  key={active.path}
                  context={active}
                  contexts={contexts}
                  navigate={navigate}
                />
              )
            ) : (
              <EmptyState
                title={path ? "Context unavailable" : "Your assistant is not available yet"}
              >
                Select a space or refresh when the runtime is ready.
              </EmptyState>
            )}
          </div>
        </SidebarInset>
      </SidebarProvider>
    </TooltipProvider>
  );
}
