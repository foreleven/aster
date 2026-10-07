import { useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { QueryKeys } from "@aster/api";
import { ApplicationClient, resultError, resultValue } from "../api/client";
import { ErrorNotice, Loading, EmptyState } from "../components/feedback";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "../components/ui/tabs";
import { Button } from "../components/ui/button";
import { Item, ItemContent, ItemTitle, ItemDescription, ItemGroup } from "../components/ui/item";
import { Markdown } from "../components/markdown";
import { dateTime } from "../contexts/model";
const inspection = Atom.family((path: string) =>
  ApplicationClient.query(
    "InspectTask",
    { path },
    { reactivityKeys: [QueryKeys.all, QueryKeys.context(path), QueryKeys.approvals] },
  ),
);
export function TaskDetails({
  path,
  navigate,
}: {
  path: string;
  navigate: (path: string) => void;
}) {
  const result = useAtomValue(inspection(path));
  const view = resultValue(result);
  const error = resultError(result);
  if (error) return <ErrorNotice error={error} />;
  if (!view) return <Loading />;
  return (
    <section className="flex flex-col gap-5" aria-label="Task details">
      <p className="text-sm text-muted-foreground">
        {view.agent === "internal" ? "Handled by Aster" : `Agent: ${view.agent}`}
      </p>
      <Tabs defaultValue="overview">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="records">Execution records</TabsTrigger>
        </TabsList>
        <TabsContent value="overview" className="flex flex-col gap-6 pt-4">
          <section className="flex flex-col gap-3">
            <h2 className="font-medium">Instructions</h2>
            <Markdown>{view.instructions}</Markdown>
          </section>
          {!view.hasExecution && (
            <p className="text-sm text-muted-foreground">
              No execution handle is recorded. The task has not been resubmitted.
            </p>
          )}
          <ErrorNotice error={view.error ?? ""} />
          {view.result && (
            <section className="flex flex-col gap-3">
              <h2 className="font-medium">Result</h2>
              <Markdown>{view.result}</Markdown>
            </section>
          )}
          {view.sources.length > 0 && (
            <section>
              <h2 className="mb-3 font-medium">Sources</h2>
              {view.sources.map((source) => (
                <p key={source}>
                  {source.startsWith("/") ? (
                    <Button variant="link" onClick={() => navigate(source)}>
                      {source}
                    </Button>
                  ) : (
                    source
                  )}
                </p>
              ))}
            </section>
          )}
        </TabsContent>
        <TabsContent value="records" className="pt-4">
          <ItemGroup className="gap-3">
            {view.messages.map((message) => (
              <Item key={message.id} variant="outline">
                <ItemContent>
                  <ItemTitle>{message.kind}</ItemTitle>
                  <ItemDescription>{dateTime(message.at)}</ItemDescription>
                  <Markdown>{message.text}</Markdown>
                </ItemContent>
              </Item>
            ))}
          </ItemGroup>
          {!view.messages.length && (
            <EmptyState title="No execution records yet">
              Records will appear as work proceeds.
            </EmptyState>
          )}
        </TabsContent>
      </Tabs>
      {view.requests.length > 0 && (
        <section>
          <h2 className="mb-3 font-medium">Requests</h2>
          <ItemGroup>
            {view.requests.map((request) => (
              <Item key={request.id} variant="outline">
                <ItemContent>
                  <ItemTitle>
                    {request.kind === "approval" ? "Approval" : "Additional information"}
                  </ItemTitle>
                  <ItemDescription>{request.prompt}</ItemDescription>
                  <p className="text-xs text-muted-foreground">
                    Response: {request.responseStatus}
                  </p>
                </ItemContent>
              </Item>
            ))}
          </ItemGroup>
        </section>
      )}
    </section>
  );
}
