import React, { useState } from "react";
import { Asterisk, ChevronDown, ChevronRight, RefreshCw, Search, X } from "lucide-react";
import type { ContextView } from "../dashboard/model";

export const contextTitle = (context: ContextView) =>
  context.path === "/goals/personal"
    ? "Personal assistant"
    : context.state.title || context.state.chat?.name || context.description || context.path;

type Node = { context: ContextView; children: Node[] };
const tree = (contexts: readonly ContextView[]) => {
  const nodes = new Map(
    contexts.map((context): [string, Node] => [context.path, { context, children: [] }]),
  );
  const roots: Node[] = [];
  for (const node of nodes.values()) {
    let parent = node.context.path.slice(0, node.context.path.lastIndexOf("/"));
    while (parent && !nodes.has(parent)) parent = parent.slice(0, parent.lastIndexOf("/"));
    const siblings = nodes.get(parent)?.children ?? roots;
    siblings.push(node);
  }
  return roots;
};

function Branch({
  node,
  selected,
  select,
  query,
}: {
  node: Node;
  selected: string;
  select: (path: string) => void;
  query: string;
}) {
  const [collapsed, setCollapsed] = useState(false);
  const { context, children } = node;
  const matches = (candidate: Node): boolean =>
    `${candidate.context.path} ${contextTitle(candidate.context)}`.toLowerCase().includes(query) ||
    candidate.children.some(matches);
  if (query && !matches(node)) return null;
  const expanded = !collapsed || !!query;
  return (
    <li>
      <div className={`context-tree-row ${selected === context.path ? "selected" : ""}`}>
        {children.length ? (
          <button
            className="context-branch-toggle"
            aria-label={`${expanded ? "Collapse" : "Expand"} ${context.path}`}
            aria-expanded={expanded}
            onClick={() => setCollapsed(!collapsed)}
          >
            {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          </button>
        ) : (
          <span className="context-branch-spacer" />
        )}
        <button
          className="context-tree-select"
          aria-current={selected === context.path ? "page" : undefined}
          onClick={() => select(context.path)}
          title={context.description}
        >
          <strong>{contextTitle(context)}</strong>
          <span>{context.path}</span>
          <small>
            {context.path.split("/")[1]} ·{" "}
            {context.revision === undefined ? "Revision unavailable" : `r${context.revision}`}
          </small>
        </button>
      </div>
      {expanded && children.length > 0 && (
        <ul>
          {children.map((child) => (
            <Branch
              key={child.context.path}
              node={child}
              selected={selected}
              select={select}
              query={query}
            />
          ))}
        </ul>
      )}
    </li>
  );
}

export function ContextNavigation({
  contexts,
  selected,
  onSelect,
  connected,
  loading,
  refresh,
  close,
}: {
  contexts: readonly ContextView[];
  selected: string;
  onSelect: (path: string) => void;
  connected: boolean;
  loading: boolean;
  refresh: () => void;
  close: () => void;
}) {
  const [query, setQuery] = useState("");
  const sorted = [...contexts].sort((a, b) => {
    const personalOrder =
      Number(b.path === "/goals/personal") - Number(a.path === "/goals/personal");
    return personalOrder || a.path.localeCompare(b.path);
  });
  return (
    <aside className="goals-navigation context-navigation">
      <div className="brand-lockup">
        <Asterisk className="aster-logo" size={32} />
        <span>
          <strong>Aster</strong>
          <small>Your personal workspace</small>
        </span>
        <button
          className="icon-button mobile-navigation-close"
          aria-label="Close contexts"
          onClick={close}
        >
          <X size={18} />
        </button>
      </div>
      <label className="context-search">
        <Search size={16} />
        <input
          aria-label="Find context"
          placeholder="Find a context…"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
      </label>
      <nav aria-label="Contexts">
        <div className="context-navigation-heading">
          <h2>Contexts</h2>
          <span>{contexts.length}</span>
        </div>
        <ul className="context-tree">
          {tree(sorted).map((node) => (
            <Branch
              key={node.context.path}
              node={node}
              selected={selected}
              select={onSelect}
              query={query.trim().toLowerCase()}
            />
          ))}
        </ul>
        {!contexts.length && <p className="quiet-message">No Contexts available.</p>}
      </nav>
      <div className="navigation-footer">
        <span>
          <strong>Local workspace</strong>
          <small>
            <i className={connected ? "connected" : ""} />
            {connected ? "Live" : "Reconnecting"}
          </small>
        </span>
        <button
          className="icon-button"
          aria-label="Refresh data"
          disabled={loading}
          onClick={refresh}
        >
          <RefreshCw size={16} />
        </button>
      </div>
    </aside>
  );
}
