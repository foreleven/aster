import { useState } from "react";
import { Search } from "lucide-react";
import { InputGroup, InputGroupAddon, InputGroupInput } from "../components/ui/input-group";
import { ToggleGroup, ToggleGroupItem } from "../components/ui/toggle-group";
import { EmptyState } from "../components/feedback";
import { ContextList } from "../contexts/list";
import { contextTitle, kindOf, type ContextView } from "../contexts/model";
export type CollectionView = "tasks" | "following" | "sources";
const sections = {
  tasks: {
    title: "Tasks",
    description: "Work your assistant is handling for you.",
    kind: "task",
    filters: ["all", "running", "needs-input", "finished"],
  },
  following: {
    title: "Following",
    description: "Scheduled tasks and things your assistant watches for.",
    kind: "signal",
    filters: ["all", "active", "paused"],
  },
  sources: {
    title: "Sources",
    description: "Information available to your assistant.",
    kind: "source",
    filters: [],
  },
} as const;
const matchesStatus = (status: string | undefined, filter: string) => {
  if (filter === "all") return true;
  if (filter === "running") return ["ready", "running"].includes(status ?? "");
  if (filter === "needs-input")
    return ["waiting_input", "failed", "uncertain"].includes(status ?? "");
  if (filter === "finished") return ["completed", "cancelled"].includes(status ?? "");
  return status === filter;
};
export function Collection({
  view,
  contexts,
  navigate,
}: {
  view: CollectionView;
  contexts: readonly ContextView[];
  navigate: (path: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState("all");
  const section = sections[view];
  const items = contexts.filter(
    (context) =>
      kindOf(context.path) === section.kind &&
      matchesStatus(context.state.status, filter) &&
      `${contextTitle(context)} ${context.path}`.toLowerCase().includes(query.trim().toLowerCase()),
  );
  return (
    <section className="mx-auto flex w-full max-w-4xl flex-col gap-7 p-5 md:p-10">
      <header className="flex flex-col gap-2">
        <h1 className="text-3xl font-medium tracking-tight">{section.title}</h1>
        <p className="text-muted-foreground">{section.description}</p>
      </header>
      <div className="flex flex-wrap items-center gap-3">
        <InputGroup className="max-w-sm">
          <InputGroupAddon>
            <Search />
          </InputGroupAddon>
          <InputGroupInput
            aria-label={`Search ${view}`}
            placeholder="Search…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </InputGroup>
        {section.filters.length > 0 && (
          <ToggleGroup
            type="single"
            value={filter}
            onValueChange={(value: string) => value && setFilter(value)}
            aria-label={`Filter ${view}`}
            variant="outline"
          >
            {section.filters.map((value) => (
              <ToggleGroupItem key={value} value={value}>
                {value.replaceAll("-", " ")}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>
        )}
      </div>
      {items.length ? (
        <ContextList contexts={items} navigate={navigate} />
      ) : (
        <EmptyState title="Nothing here yet">
          {query || filter !== "all"
            ? "Try another search or filter."
            : "Your assistant’s work will appear here as it becomes available."}
        </EmptyState>
      )}
    </section>
  );
}
