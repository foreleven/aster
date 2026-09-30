import React from "react";
import { Badge } from "@/components/ui/badge";
import { Empty, EmptyHeader, EmptyTitle, EmptyDescription } from "@/components/ui/empty";
import { Button } from "@/components/ui/button";
import { label, time } from "@/lib/dashboard";
export function Status({ value }: { value?: string }) {
  return (
    <Badge
      variant={
        ["failed", "uncertain", "preparation-failed"].includes(value ?? "")
          ? "destructive"
          : ["running", "active", "processing", "completed"].includes(value ?? "")
            ? "secondary"
            : "outline"
      }
    >
      {value === "processing" ? "Processing" : label(value)}
    </Badge>
  );
}
export function Blank({
  title = "No records yet",
  detail = "New Context and execution events appear here automatically.",
}) {
  return (
    <Empty>
      <EmptyHeader>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{detail}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}
export function Messages({
  messages = [],
  inspect,
}: {
  messages?: readonly import("./model").MessageView[];
  inspect: (path: string) => void;
}) {
  return messages.length ? (
    <div>
      {messages.map((message, i) => (
        <article className="message" key={i}>
          <div className="flex items-center justify-between gap-3 mb-2">
            <Badge variant="outline">{message.label}</Badge>
            <time className="mono text-muted-foreground">
              {time(message.at ?? message.timestamp)}
            </time>
          </div>
          {message.tool ? (
            <details>
              <summary className="cursor-pointer text-sm">
                {message.toolName || "View tool call"}
              </summary>
              <pre>{message.details}</pre>
            </details>
          ) : message.text !== undefined ? (
            <p>{message.text}</p>
          ) : (
            <pre>{message.details}</pre>
          )}
          {message.references.map((path) => (
            <Button key={path} size="sm" variant="link" onClick={() => inspect(path)}>
              {path}
            </Button>
          ))}
        </article>
      ))}
    </div>
  ) : (
    <Blank title="No messages yet" />
  );
}
