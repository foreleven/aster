import React from "react";
import { Match } from "effect";
import {
  Archive,
  CircleCheck,
  CirclePlay,
  Eye,
  Hourglass,
  MessageSquare,
  Orbit,
  Search,
  Settings,
  FileText,
} from "lucide-react";
import type { ContextView, MessageView } from "../dashboard/model";
import { label } from "../lib/dashboard";

export const goalGroups = ["Active", "Paused", "Watching", "Archived", "Other"] as const;
export const goalGroup = (status?: string) =>
  Match.value(status).pipe(
    Match.when("active", () => "Active" as const),
    Match.when("paused", () => "Paused" as const),
    Match.when("watching", () => "Watching" as const),
    Match.whenOr("completed", "archived", () => "Archived" as const),
    Match.orElse(() => "Other" as const),
  );
export const slugFor = (goal: ContextView) => goal.path.slice("/goals/".length);
export const lastActivity = (context: ContextView) =>
  context.messages.at(-1)?.at ?? context.messages.at(-1)?.timestamp;
export const dateLabel = (value?: string | number) => {
  if (value === undefined) return "Not recorded";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? "Not recorded"
    : date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
};
export const clockLabel = (value?: string | number) => {
  if (value === undefined) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? "—"
    : date.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
};
export const statusLabel = (status?: string) =>
  Match.value(status).pipe(
    Match.when("active", () => "Active"),
    Match.when("paused", () => "Paused"),
    Match.when("watching", () => "Watching"),
    Match.when("archived", () => "Archived"),
    Match.when("waiting_input", () => "Waiting for input"),
    Match.orElse((value) => label(value)),
  );
export const statusTone = (status?: string) =>
  Match.value(status).pipe(
    Match.whenOr("active", "completed", "acknowledged", "ready", () => "green"),
    Match.whenOr("running", "submitting", "processing", () => "blue"),
    Match.whenOr(
      "uncertain",
      "waiting_input",
      "awaiting-confirmation",
      "blocked",
      "pending",
      () => "amber",
    ),
    Match.whenOr("failed", "preparation-failed", "rejected", () => "red"),
    Match.orElse(() => "neutral"),
  );
export function Pill({ status }: { status?: string }) {
  return <span className={`goal-pill tone-${statusTone(status)}`}>{statusLabel(status)}</span>;
}
export function WorkIcon({ status }: { status?: string }) {
  const Icon = Match.value(status).pipe(
    Match.whenOr("running", "submitting", () => CirclePlay),
    Match.when("completed", () => CircleCheck),
    Match.whenOr("waiting_input", "awaiting-confirmation", "pending", () => Hourglass),
    Match.orElse(() => Settings),
  );
  const tone = Match.value(status).pipe(
    Match.whenOr("running", "submitting", "completed", () => "green"),
    Match.when("uncertain", () => "purple"),
    Match.orElse(statusTone),
  );
  return <Icon className={`work-icon tone-${tone}`} size={21} aria-hidden="true" />;
}
export const eventKind = (message: MessageView) =>
  Match.value(message.label.toLowerCase()).pipe(
    Match.whenOr("user", "you", () => ({
      label: "You",
      icon: MessageSquare,
      tone: "blue",
      category: "notes",
    })),
    Match.whenOr("signal", "triggered", () => ({
      label: "Signal",
      icon: Search,
      tone: "amber",
      category: "signals",
    })),
    Match.whenOr("task", "taskprepared", () => ({
      label: "Task",
      icon: CirclePlay,
      tone: "green",
      category: "tasks",
    })),
    Match.whenOr("result", "tool result", "completed", () => ({
      label: "Result",
      icon: FileText,
      tone: "neutral",
      category: "results",
    })),
    Match.whenOr("approval", "confirmationrequested", () => ({
      label: "Approval",
      icon: CircleCheck,
      tone: "green",
      category: "progress",
    })),
    Match.when("execution", () => ({
      label: "Execution",
      icon: Settings,
      tone: "purple",
      category: "tasks",
    })),
    Match.when("progress", () => ({
      label: "Progress",
      icon: Eye,
      tone: "purple",
      category: "progress",
    })),
    Match.when("error", () => ({
      label: "Error",
      icon: Archive,
      tone: "red",
      category: "progress",
    })),
    Match.orElse(() => ({ label: "Aster", icon: Orbit, tone: "blue", category: "progress" })),
  );
export function EmptyState({ title, children }: { title: string; children?: React.ReactNode }) {
  return (
    <div className="goals-empty">
      <Orbit size={28} aria-hidden="true" />
      <h2>{title}</h2>
      {children && <p>{children}</p>}
    </div>
  );
}
