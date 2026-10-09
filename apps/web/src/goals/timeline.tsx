import { useMemo } from "react";
import { useAtomValue, useAtomSet, useAtomRefresh } from "@effect/atom-react";
import { goalTimeline } from "../api/timeline";
import { resultError, resultValue } from "../api/client";
import { Button } from "../components/ui/button";
import { Message, MessageContent, MessageFooter } from "../components/ui/message";
import { Bubble, BubbleContent } from "../components/ui/bubble";
import {
  MessageScroller,
  MessageScrollerProvider,
  MessageScrollerViewport,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerButton,
} from "../components/ui/message-scroller";
import { Markdown } from "../components/markdown";
import { ErrorNotice, Loading } from "../components/feedback";
import { dateTime } from "../contexts/model";

export function Timeline({ slug }: { slug: string }) {
  const atoms = useMemo(() => goalTimeline(slug), [slug]);
  const result = useAtomValue(atoms.feed);
  const more = useAtomSet(atoms.before);
  const refresh = useAtomRefresh(atoms.latest);
  const refreshFeed = useAtomRefresh(atoms.feed);
  const page = resultValue(result);
  const error = resultError(result);
  if (!page && !error) return <Loading />;
  return (
    <MessageScrollerProvider autoScroll defaultScrollPosition="end">
      <MessageScroller>
        <MessageScrollerViewport>
          <MessageScrollerContent className="mx-auto max-w-3xl gap-8 px-5 py-8 md:px-8">
            {error && (
              <ErrorNotice
                error={error}
                retry={() => {
                  refresh();
                  refreshFeed();
                }}
              />
            )}
            {page?.nextBefore !== null && page?.nextBefore !== undefined && (
              <Button
                className="self-center"
                variant="outline"
                size="sm"
                disabled={result.waiting}
                onClick={() => more(page.nextBefore ?? undefined)}
              >
                Load earlier messages
              </Button>
            )}
            {page?.messages.map((message) => (
              <MessageScrollerItem key={message.id} messageId={String(message.id)}>
                <Message
                  align={message.role === "user" ? "end" : "start"}
                  aria-label={`${message.role} message`}
                >
                  <MessageContent>
                    <Bubble
                      align={message.role === "user" ? "end" : "start"}
                      variant={message.role === "user" ? "secondary" : "ghost"}
                    >
                      <BubbleContent>
                        <Markdown>{message.text}</Markdown>
                      </BubbleContent>
                    </Bubble>
                    <MessageFooter>
                      <time dateTime={message.at}>{dateTime(message.at)}</time>
                    </MessageFooter>
                  </MessageContent>
                </Message>
              </MessageScrollerItem>
            ))}
          </MessageScrollerContent>
        </MessageScrollerViewport>
        <MessageScrollerButton />
      </MessageScroller>
    </MessageScrollerProvider>
  );
}
