import type { ReactNode } from "react";
import { Alert, AlertDescription, AlertTitle } from "./ui/alert";
import { Badge } from "./ui/badge";
import { Empty, EmptyHeader, EmptyTitle, EmptyDescription } from "./ui/empty";
import { Skeleton } from "./ui/skeleton";
import { Button } from "./ui/button";

export function EmptyState({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <Empty>
      <EmptyHeader>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{children}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}
export function ErrorNotice({ error, retry }: { error: string; retry?: () => void }) {
  if (!error) return null;
  return (
    <Alert variant="destructive">
      <AlertTitle>Something needs attention</AlertTitle>
      <AlertDescription>
        {error}
        {retry && (
          <Button variant="outline" size="sm" onClick={retry}>
            Retry
          </Button>
        )}
      </AlertDescription>
    </Alert>
  );
}
export function Loading() {
  return (
    <div role="status" aria-label="Loading" className="flex flex-col gap-4 p-6">
      <Skeleton className="h-5 w-1/3" />
      <Skeleton className="h-20 w-full" />
      <Skeleton className="h-20 w-3/4" />
    </div>
  );
}
export function Status({ value }: { value?: string }) {
  if (!value) return null;
  return (
    <Badge variant={["failed", "uncertain"].includes(value) ? "destructive" : "secondary"}>
      {value.replaceAll("_", " ")}
    </Badge>
  );
}
