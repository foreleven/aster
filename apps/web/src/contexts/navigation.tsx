import { useState } from "react";
import { useAtomValue } from "@effect/atom-react";
import {
  Asterisk,
  Inbox,
  Layers,
  ListChecks,
  MessageCircle,
  Radio,
  RefreshCw,
  Circle,
} from "lucide-react";
import {
  Sidebar,
  SidebarHeader,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuItem,
  SidebarMenuButton,
  SidebarMenuBadge,
  SidebarInput,
  useSidebar,
} from "../components/ui/sidebar";
import { Button } from "../components/ui/button";
import { approvalEntries } from "../api/state";
import { contextTitle, kindOf, type ContextView } from "./model";
import type { CollectionView } from "../assistant/collection";
export type WorkspaceView = CollectionView | "approvals";
export function ContextNavigation({
  contexts,
  selected,
  view,
  navigate,
  selectView,
  connected,
  refresh,
  loading,
}: {
  contexts: readonly ContextView[];
  selected: string;
  view: string;
  navigate: (path: string) => void;
  selectView: (view: WorkspaceView) => void;
  connected: boolean;
  refresh: () => void;
  loading: boolean;
}) {
  const [query, setQuery] = useState("");
  const { setOpenMobile } = useSidebar();
  const approvals = useAtomValue(approvalEntries);
  const count = approvals.filter((entry) => entry.status === "pending").length;
  const choose = (path: string) => {
    navigate(path);
    setOpenMobile(false);
    setQuery("");
  };
  const spaces = contexts.filter(
    (context) => kindOf(context.path) === "goal" && context.path !== "/goals/personal",
  );
  const results = contexts.filter((context) =>
    `${contextTitle(context)} ${context.path}`.toLowerCase().includes(query.trim().toLowerCase()),
  );
  return (
    <Sidebar collapsible="offcanvas">
      <SidebarHeader className="gap-5 p-5">
        <div className="flex items-center gap-2 py-2">
          <Asterisk />
          <span className="text-2xl font-semibold tracking-tight">aster</span>
        </div>
        <SidebarInput
          aria-label="Find context"
          placeholder="Find anything…"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
      </SidebarHeader>
      <SidebarContent>
        <nav aria-label="Workspace">
          {query.trim() ? (
            <SidebarGroup>
              <SidebarGroupLabel>Search results</SidebarGroupLabel>
              <SidebarMenu>
                {results.map((context) => (
                  <SidebarMenuItem key={context.path}>
                    <SidebarMenuButton onClick={() => choose(context.path)} tooltip={context.path}>
                      <span>{contextTitle(context)}</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                ))}
              </SidebarMenu>
              {!results.length && <p className="p-2 text-sm text-muted-foreground">No matches.</p>}
            </SidebarGroup>
          ) : (
            <>
              <SidebarGroup>
                <SidebarMenu>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      isActive={!view && selected === "/goals/personal"}
                      onClick={() => choose("/goals/personal")}
                    >
                      <MessageCircle />
                      <span>Your assistant</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  {(
                    [
                      { view: "tasks", label: "Tasks", icon: ListChecks },
                      { view: "following", label: "Following", icon: Radio },
                      { view: "approvals", label: "Needs you", icon: Inbox },
                      { view: "sources", label: "Sources", icon: Layers },
                    ] as const
                  ).map((item) => (
                    <SidebarMenuItem key={item.view}>
                      <SidebarMenuButton
                        isActive={view === item.view}
                        onClick={() => {
                          selectView(item.view);
                          setOpenMobile(false);
                        }}
                      >
                        <item.icon />
                        <span>{item.label}</span>
                      </SidebarMenuButton>
                      {item.view === "approvals" && count > 0 && (
                        <SidebarMenuBadge>{count}</SidebarMenuBadge>
                      )}
                    </SidebarMenuItem>
                  ))}
                </SidebarMenu>
              </SidebarGroup>
              <SidebarGroup>
                <SidebarGroupLabel>Your spaces</SidebarGroupLabel>
                <SidebarGroupContent>
                  <SidebarMenu>
                    {spaces.map((goal) => (
                      <SidebarMenuItem key={goal.path}>
                        <SidebarMenuButton
                          isActive={!view && selected === goal.path}
                          onClick={() => choose(goal.path)}
                          tooltip={contextTitle(goal)}
                        >
                          <Circle />
                          <span>{contextTitle(goal)}</span>
                        </SidebarMenuButton>
                      </SidebarMenuItem>
                    ))}
                  </SidebarMenu>
                </SidebarGroupContent>
              </SidebarGroup>
            </>
          )}
        </nav>
      </SidebarContent>
      <SidebarFooter className="p-4">
        <div className="flex items-center justify-between gap-2">
          <div>
            <p className="text-sm">Personal workspace</p>
            <p className="text-xs text-muted-foreground" role="status">
              {connected ? "Live" : "Reconnecting"}
            </p>
          </div>
          <Button
            variant="ghost"
            size="icon"
            aria-label="Refresh data"
            disabled={loading}
            onClick={refresh}
          >
            <RefreshCw />
          </Button>
        </div>
      </SidebarFooter>
    </Sidebar>
  );
}
