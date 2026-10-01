import React from "react";
import { Archive, Asterisk, Eye, Orbit, Pause, RefreshCw, UserRound, X } from "lucide-react";
import { Match } from "effect";
import type { ContextView } from "../dashboard/model";
import { dateLabel, goalGroup, goalGroups, lastActivity, slugFor } from "./presentation";

export function GoalsNavigation({
  goals,
  selected,
  onSelect,
  connected,
  loading,
  refresh,
  close,
}: {
  goals: readonly ContextView[];
  selected: string;
  onSelect: (path: string) => void;
  connected: boolean;
  loading: boolean;
  refresh: () => void;
  close: () => void;
}) {
  return (
    <aside className="goals-navigation">
      <div className="brand-lockup">
        <Asterisk className="aster-logo" size={36} strokeWidth={1.9} />
        <span>
          <strong>Aster</strong>
          <small>Your local AI assistant</small>
        </span>
        <button
          className="icon-button mobile-navigation-close"
          aria-label="Close goals"
          onClick={close}
        >
          <X size={18} />
        </button>
      </div>
      <div className="goals-navigation-current">
        <Orbit size={20} />
        Goals
      </div>
      <nav aria-label="Goals">
        <p className="navigation-title">Goals</p>
        {goalGroups.map((group) => {
          const entries = goals.filter((goal) => goalGroup(goal.state.status) === group);
          if (!entries.length) return null;
          const Icon = Match.value(group).pipe(
            Match.when("Archived", () => Archive),
            Match.when("Paused", () => Pause),
            Match.when("Watching", () => Eye),
            Match.orElse(() => Orbit),
          );
          return (
            <section className="goal-group" key={group}>
              <h2>
                {group}
                <b>{entries.length}</b>
              </h2>
              {entries.map((goal) => {
                const activity = lastActivity(goal);
                return (
                  <button
                    className={`goal-list-item ${selected === goal.path ? "selected" : ""}`}
                    aria-current={selected === goal.path ? "page" : undefined}
                    key={goal.path}
                    onClick={() => onSelect(goal.path)}
                    title={goal.description}
                  >
                    <Icon size={21} className="goal-list-icon" />
                    <span>
                      <strong>{goal.description || slugFor(goal)}</strong>
                      <small>
                        {group}
                        {activity !== undefined ? ` · ${dateLabel(activity)}` : ""}
                      </small>
                    </span>
                    {group === "Active" && <i aria-label="Active" />}
                  </button>
                );
              })}
            </section>
          );
        })}
      </nav>
      <div className="navigation-footer">
        <UserRound size={28} />
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
          title="Refresh data"
          disabled={loading}
          onClick={refresh}
        >
          <RefreshCw size={16} />
        </button>
      </div>
    </aside>
  );
}
