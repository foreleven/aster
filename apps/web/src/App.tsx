import {
  useAtomMount,
  useAtomRefresh,
  useAtomSet,
  useAtomValue,
  RegistryContext,
} from "@effect/atom-react";
import { AsyncResult } from "effect/unstable/reactivity";
import { QueryKeys } from "@aster/api-contracts";
import { invalidateQueries } from "./api/client";
import {
  dashboardRows,
  dashboardStatus,
  runtimeView,
  runtimeEvents,
  pendingApprovals,
  failureCount,
  selectedRow,
} from "./dashboard/state";
import {
  eventPath,
  filterRows,
  type DashboardPage,
  type RowFilter,
  type RuntimeActor,
} from "./dashboard/model";
import { connection, telemetryRefresh } from "./api/events";
import React, { useState, useContext } from "react";
import {
  Activity,
  LayoutDashboard,
  Network,
  Target,
  Zap,
  Inbox,
  RefreshCw,
  ArrowUpRight,
  Search,
  ChevronRight,
  Radio,
  Database,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableHeader,
  TableBody,
  TableHead,
  TableRow,
  TableCell,
} from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Inspector } from "./dashboard/inspector";
import { Approvals } from "./dashboard/approvals";
import { Status, Blank } from "./dashboard/shared";
import { time } from "./lib/dashboard";
const pages: readonly { id: DashboardPage; title: string; icon: typeof Activity }[] = [
  { id: "overview", title: "Overview", icon: LayoutDashboard },
  { id: "actors", title: "Actors", icon: Network },
  { id: "goals", title: "Goals", icon: Target },
  { id: "signals", title: "Signals", icon: Zap },
  { id: "approvals", title: "Approvals", icon: Inbox },
];
function Topology({
  actors,
  inspect,
}: {
  actors: readonly RuntimeActor[];
  inspect: (path: string) => void;
}) {
  function children(parent: string, depth = 0): React.ReactNode {
    return actors
      .filter((a) => a.parent === parent)
      .map((a) => (
        <div className="tree-node" key={a.incarnation}>
          <button className="tree-label" onClick={() => inspect(a.path)}>
            <span className={a.processing ? "dot live" : "dot"} />
            <span className="mono truncate flex-1">{a.path.split("/").at(-1)}</span>
            <span className="text-xs text-muted-foreground">
              {a.processing ? "Processing" : a.status === "running" ? "Idle" : a.status}
            </span>
            <ChevronRight className="size-3" />
          </button>
          {depth < 30 && children(a.path, depth + 1)}
        </div>
      ));
  }
  return (
    <div className="topology">
      <div className="tree-root mono">
        <Radio className="size-4 mr-2 text-primary" /> /user
      </div>
      {actors.length ? (
        <div className="forest">{children("/user")}</div>
      ) : (
        <Blank
          title="No running Actors"
          detail="Start Aster to see running Actors in the topology."
        />
      )}
    </div>
  );
}
export default function App() {
  const [actionError, setActionError] = useState("");
  const [page, setPage] = useState<DashboardPage>("overview"),
    [selected, setSelected] = useState("");
  const [query, setQuery] = useState(""),
    [filter, setFilter] = useState<RowFilter>("all");
  const registry = useContext(RegistryContext);
  const { connected, error, loading, loaded, at } = useAtomValue(dashboardStatus);
  const rows = useAtomValue(dashboardRows);
  const runtime = useAtomValue(runtimeView);
  const actors = runtime?.actors ?? [];
  const events = useAtomValue(runtimeEvents);
  const pending = useAtomValue(pendingApprovals);
  const failures = useAtomValue(failureCount);
  useAtomMount(telemetryRefresh);
  const invalidate = useAtomSet(invalidateQueries);
  const reconnect = useAtomRefresh(connection);
  const load = () => {
    invalidate([QueryKeys.all]);
    if (AsyncResult.isFailure(registry.get(connection))) reconnect();
  };
  const inspect = (path: string) => {
    if (!registry.get(selectedRow(path))) {
      setActionError(`Context not found: ${path}`);
      return;
    }
    setSelected(path);
  };
  const filtered = filterRows(rows, page, query, filter);
  const navigate = (id: DashboardPage) => {
    setPage(id);
    setQuery("");
    setFilter("all");
  };
  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <Activity className="brand-mark size-8" />
          Aster
          <span className="text-muted-foreground text-xs font-normal ml-auto">LOCAL</span>
        </div>
        <nav aria-label="Main navigation">
          {pages.map((p) => (
            <Button
              key={p.id}
              className="nav-button"
              variant={page === p.id ? "secondary" : "ghost"}
              onClick={() => navigate(p.id)}
            >
              <p.icon data-icon="inline-start" />
              {p.title}
              {p.id === "approvals" && pending > 0 && (
                <Badge className="ml-auto" variant="outline">
                  {pending}
                </Badge>
              )}
            </Button>
          ))}
        </nav>
        <div className="sidebar-footer mt-auto border-t pt-5 text-xs text-muted-foreground flex flex-col gap-3">
          <div className="flex items-center gap-2">
            <Database className="size-4" />
            Local ActorSystem
          </div>
          <p>Context → Signal → Agent</p>
          <p>Data stored locally</p>
        </div>
      </aside>
      <div className="workspace">
        <header className="topbar">
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            Workspace
            <ChevronRight className="size-3" />
            <span className="text-foreground">{pages.find((p) => p.id === page)?.title}</span>
          </div>
          <div className="flex items-center gap-4">
            <span className="flex items-center gap-2 text-xs">
              <i className={connected ? "dot live" : "dot"} />
              {connected ? "Live" : "Reconnecting"}
            </span>
            <Button
              size="icon-sm"
              variant="ghost"
              aria-label="Refresh data"
              disabled={loading}
              onClick={load}
            >
              <RefreshCw />
            </Button>
          </div>
        </header>
        <main className="content">
          <div className="flex items-start justify-between gap-4">
            <div>
              <h1>{pages.find((p) => p.id === page)?.title}</h1>
              <p className="subtitle">
                {
                  {
                    overview: "Track every Actor from context to action.",
                    actors: "Browse live runtime state and persisted Contexts.",
                    goals: "Follow Goal progress, reasoning, execution, and feedback.",
                    signals: "Follow the task workflow from trigger to execution result.",
                    approvals:
                      "Manage task confirmations, execution approvals, and requests for information.",
                  }[page]
                }
              </p>
            </div>
            <span className="mono text-muted-foreground pt-2 hidden sm:block">
              Updated {time(at)}
            </span>
          </div>
          {(error || actionError) && (
            <Alert variant="destructive" className="my-4">
              <AlertTitle>Could not load data or complete the action</AlertTitle>
              <AlertDescription>
                {error || actionError}{" "}
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => {
                    setActionError("");
                    load();
                  }}
                >
                  Retry
                </Button>
              </AlertDescription>
            </Alert>
          )}
          {!loaded && !error ? (
            <div className="grid gap-4 mt-8">
              <Skeleton className="h-28" />
              <Skeleton className="h-80" />
            </div>
          ) : (
            <>
              {page === "overview" && (
                <>
                  <div className="metrics">
                    {(
                      [
                        {
                          name: "Running Actors",
                          value: actors.length,
                          detail: "Instances in this process",
                          page: "actors",
                        },
                        {
                          name: "Processing",
                          value: actors.filter((a) => a.processing).length,
                          detail: "Handling a Command",
                          page: "actors",
                        },
                        {
                          name: "Pending approval",
                          value: pending,
                          detail: "Needs your confirmation",
                          page: "approvals",
                        },
                        {
                          name: "Execution issues",
                          value: failures,
                          detail: "Failed or uncertain outcomes",
                          page: "signals",
                        },
                      ] satisfies readonly {
                        name: string;
                        value: number;
                        detail: string;
                        page: DashboardPage;
                      }[]
                    ).map((m) => (
                      <button
                        className="metric text-left"
                        key={m.name}
                        onClick={() => navigate(m.page)}
                      >
                        <div className="text-xs text-muted-foreground flex justify-between">
                          {m.name}
                          <ArrowUpRight className="size-3" />
                        </div>
                        <strong>{m.value}</strong>
                        <span className="text-xs text-muted-foreground">{m.detail}</span>
                      </button>
                    ))}
                  </div>
                  <div className="overview-grid">
                    <section className="panel">
                      <div className="panel-heading">
                        <h2>Actor topology</h2>
                        <Badge variant="outline">{actors.length} instances</Badge>
                      </div>
                      {runtime ? (
                        <Topology actors={actors} inspect={inspect} />
                      ) : (
                        <Blank
                          title="Runtime inspection unavailable"
                          detail="Restart the updated Aster app to enable runtime inspection."
                        />
                      )}
                    </section>
                    <section className="panel">
                      <div className="panel-heading">
                        <h2>Recent activity</h2>
                        <Activity className="size-4 text-muted-foreground" />
                      </div>
                      <div className="activity">
                        {events.length ? (
                          events.slice(0, 40).map((e, i) => (
                            <button
                              key={i}
                              className="activity-item block w-full text-left"
                              onClick={() => inspect(eventPath(e))}
                            >
                              <div className="flex justify-between gap-2 text-xs">
                                <span>
                                  {e._tag === "CommandProcessed"
                                    ? e.commandTag || "Command"
                                    : e._tag}
                                </span>
                                <time className="mono text-muted-foreground">
                                  {time(e.timestamp)}
                                </time>
                              </div>
                              <p className="mono text-muted-foreground truncate mt-1">
                                {eventPath(e)}
                              </p>
                              {e._tag === "CommandProcessed" && e.success === false && (
                                <span className="text-xs text-destructive">Processing failed</span>
                              )}
                            </button>
                          ))
                        ) : (
                          <Blank
                            title="Waiting for runtime events"
                            detail="Shows the latest 200 events from this process."
                          />
                        )}
                      </div>
                    </section>
                  </div>
                </>
              )}
              {page === "approvals" ? (
                <div className="mt-7">
                  <Approvals inspect={inspect} report={setActionError} />
                </div>
              ) : (
                <section className="panel mt-6">
                  <div className="panel-heading flex-wrap">
                    <h2>
                      {page === "overview"
                        ? "All Actors and Contexts"
                        : page === "goals"
                          ? "Active Goals"
                          : page === "signals"
                            ? "Signals and execution records"
                            : "Actor directory"}{" "}
                      <span className="text-muted-foreground font-normal ml-2">
                        {filtered.length}
                      </span>
                    </h2>
                    <div className="flex items-center gap-3 flex-wrap">
                      <Tabs
                        value={filter}
                        onValueChange={(value) => {
                          if (value === "all" || value === "live" || value === "archived")
                            setFilter(value);
                        }}
                      >
                        <TabsList>
                          <TabsTrigger value="all">All</TabsTrigger>
                          <TabsTrigger value="live">Running</TabsTrigger>
                          <TabsTrigger value="archived">Persisted</TabsTrigger>
                        </TabsList>
                      </Tabs>
                      <div className="relative">
                        <Search className="size-4 absolute left-3 top-2.5 text-muted-foreground" />
                        <Input
                          aria-label="Search Actors"
                          className="pl-9 w-52"
                          placeholder="Search paths or descriptions…"
                          value={query}
                          onChange={(e) => setQuery(e.target.value)}
                        />
                      </div>
                    </div>
                  </div>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="pl-6">Actor / Context</TableHead>
                        <TableHead>Runtime state</TableHead>
                        <TableHead>Domain state</TableHead>
                        <TableHead>Mailbox</TableHead>
                        <TableHead>Processed</TableHead>
                        <TableHead>Messages</TableHead>
                        <TableHead className="pr-6">Details</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {filtered.map((r) => (
                        <TableRow key={r.path}>
                          <TableCell className="pl-6 max-w-80">
                            <div className="font-medium truncate">
                              {r.context?.description || r.path.split("/").at(-1)}
                            </div>
                            <div className="mono text-muted-foreground truncate mt-1">{r.path}</div>
                          </TableCell>
                          <TableCell>
                            <Status value={r.status} />
                          </TableCell>
                          <TableCell>
                            <Status value={r.context?.state?.status} />
                          </TableCell>
                          <TableCell className="mono">{r.actor?.mailboxSize ?? "—"}</TableCell>
                          <TableCell className="mono">{r.actor?.processed ?? "—"}</TableCell>
                          <TableCell className="mono">
                            {r.context?.messages.length ?? "—"}
                          </TableCell>
                          <TableCell>
                            <Button
                              variant="ghost"
                              size="icon-sm"
                              aria-label={`View ${r.path}`}
                              onClick={() => inspect(r.path)}
                            >
                              <ArrowUpRight />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                  {filtered.length === 0 && (
                    <Blank
                      title={query ? "No matching results" : "No records yet"}
                      detail={
                        query
                          ? "Try another path or description."
                          : "Data appears after the corresponding Actors are configured and started."
                      }
                    />
                  )}
                  <div className="px-6 py-3 border-t text-xs text-muted-foreground">
                    Runtime refreshes every 3 seconds · Context changes stream live · Not running
                    means only persisted records remain
                  </div>
                </section>
              )}
            </>
          )}
        </main>
      </div>
      <Inspector
        key={selected}
        path={selected}
        close={() => setSelected("")}
        inspect={inspect}
        report={setActionError}
      />
    </div>
  );
}
