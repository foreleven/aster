import { ChevronRight, Circle, ListChecks, Radio, Layers } from "lucide-react";
import {
  Item,
  ItemContent,
  ItemDescription,
  ItemGroup,
  ItemMedia,
  ItemTitle,
  ItemActions,
} from "../components/ui/item";
import { Badge } from "../components/ui/badge";
import { Status } from "../components/feedback";
import { contextTitle, kindOf, summaryText, type ContextView } from "./model";

export function ContextList({
  contexts,
  navigate,
}: {
  contexts: readonly ContextView[];
  navigate: (path: string) => void;
}) {
  return (
    <ItemGroup className="gap-2">
      {contexts.map((context) => {
        const kind = kindOf(context.path);
        const Icon = {
          goal: Circle,
          task: ListChecks,
          signal: Radio,
          source: Layers,
          system: Layers,
        }[kind];
        return (
          <Item key={context.path} variant="outline" asChild>
            <button
              type="button"
              onClick={() => navigate(context.path)}
              className="w-full text-left"
              aria-label={contextTitle(context)}
            >
              <ItemMedia variant="icon">
                <Icon />
              </ItemMedia>
              <ItemContent className="min-w-0">
                <ItemTitle className="max-w-full [overflow-wrap:anywhere]">
                  {contextTitle(context)}
                </ItemTitle>
                <ItemDescription className="[overflow-wrap:anywhere]">
                  {summaryText(context.state.summary) || context.path}
                </ItemDescription>
              </ItemContent>
              <ItemActions>
                {context.projection?.visibility === "restricted" ? (
                  <Badge variant="outline">Restricted</Badge>
                ) : (
                  <Status value={context.state.status} />
                )}
                <ChevronRight />
              </ItemActions>
            </button>
          </Item>
        );
      })}
    </ItemGroup>
  );
}
