import { useRef, useState, useLayoutEffect } from "react";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { Atom } from "effect/reactivity";
import { contextQueryKeys } from "@aster/api";
import { ArrowUp, Plus, LoaderCircle } from "lucide-react";
import { sendGoalMessage } from "../api/client";
import {
  InputGroup,
  InputGroupTextarea,
  InputGroupAddon,
  InputGroupButton,
} from "../components/ui/input-group";
import { Field, FieldGroup, FieldLabel } from "../components/ui/field";
import { Button } from "../components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
  SheetTrigger,
} from "../components/ui/sheet";
import { ErrorNotice } from "../components/feedback";
import { ContextList } from "../contexts/list";
import type { ContextView } from "../contexts/model";

// Drafts and uncertain admission identities belong to the conversation, not a mounted form.
export const drafts = Atom.make<
  Record<string, { text: string; request?: { text: string; requestId: string } }>
>({}).pipe(Atom.keepAlive);
export function Composer({
  slug,
  contexts,
  suggestions = false,
}: {
  slug: string;
  contexts: readonly ContextView[];
  suggestions?: boolean;
}) {
  const all = useAtomValue(drafts);
  const setDrafts = useAtomSet(drafts);
  const draft = all[slug] ?? { text: "" };
  const send = useAtomSet(sendGoalMessage, { mode: "promise" });
  const busy = useAtomValue(sendGoalMessage).waiting;
  const lock = useRef(false);
  const focusAfterSend = useRef(false);
  const input = useRef<HTMLTextAreaElement>(null);
  const [error, setError] = useState("");
  const [picker, setPicker] = useState(false);
  const setText = (text: string) =>
    setDrafts((previous) => ({ ...previous, [slug]: { ...previous[slug], text } }));
  useLayoutEffect(() => {
    if (!busy && focusAfterSend.current && input.current) {
      input.current.focus();
      focusAfterSend.current = false;
    }
  }, [busy, draft.text]);
  async function submit() {
    if (lock.current || busy || !draft.text.trim()) return;
    lock.current = true;
    setError("");
    const text = draft.text.trim();
    const request =
      draft.request?.text === text ? draft.request : { text, requestId: crypto.randomUUID() };
    setDrafts((previous) => ({ ...previous, [slug]: { text: draft.text, request } }));
    try {
      await send({
        payload: { slug, ...request },
        reactivityKeys: contextQueryKeys(`/goals/${slug}`),
      });
      focusAfterSend.current = true;
      setDrafts((previous) => ({ ...previous, [slug]: { text: "" } }));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      lock.current = false;
    }
  }
  return (
    <div className="flex flex-col gap-3">
      <ErrorNotice error={error} />
      <form
        onSubmit={(e) => {
          e.preventDefault();
          void submit();
        }}
      >
        <FieldGroup>
          <Field data-disabled={busy}>
            <FieldLabel className="sr-only" htmlFor={`message-${slug}`}>
              Message your assistant
            </FieldLabel>
            <InputGroup>
              <InputGroupTextarea
                id={`message-${slug}`}
                ref={input}
                aria-label="Message your assistant"
                placeholder="What’s on your mind?"
                rows={3}
                maxLength={8000}
                value={draft.text}
                disabled={busy}
                onChange={(e) => setText(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
                    e.preventDefault();
                    void submit();
                  }
                }}
              />
              <InputGroupAddon align="block-end">
                <Sheet open={picker} onOpenChange={setPicker}>
                  <SheetTrigger asChild>
                    <InputGroupButton variant="ghost" size="sm" type="button" disabled={busy}>
                      <Plus data-icon="inline-start" />
                      Add context
                    </InputGroupButton>
                  </SheetTrigger>
                  <SheetContent className="overflow-y-auto">
                    <SheetHeader>
                      <SheetTitle>Add context</SheetTitle>
                      <SheetDescription>
                        Insert a reference for your assistant to read.
                      </SheetDescription>
                    </SheetHeader>
                    <div className="p-4">
                      <ContextList
                        contexts={contexts.filter(
                          (context) =>
                            context.path !== `/goals/${slug}` &&
                            context.projection?.visibility !== "restricted",
                        )}
                        navigate={(path) => {
                          setText(`${draft.text}${draft.text ? "\n" : ""}${path}`);
                          setPicker(false);
                        }}
                      />
                    </div>
                  </SheetContent>
                </Sheet>
                <span className="ml-auto hidden text-xs text-muted-foreground sm:inline">
                  Enter to send · Shift + Enter for a new line
                </span>
                <InputGroupButton
                  aria-label="Send"
                  type="submit"
                  variant="default"
                  size="icon-sm"
                  className="ml-auto sm:ml-0"
                  disabled={busy || !draft.text.trim()}
                >
                  {busy ? <LoaderCircle className="animate-spin" /> : <ArrowUp />}
                </InputGroupButton>
              </InputGroupAddon>
            </InputGroup>
          </Field>
        </FieldGroup>
      </form>
      {suggestions && (
        <div className="flex flex-wrap justify-center gap-2">
          {[
            {
              label: "Catch me up",
              text: "What changed across my goals, and what needs my attention?",
            },
            { label: "Help me plan", text: "Help me turn what’s on my mind into a clear plan." },
            {
              label: "Explore an idea",
              text: "I have an idea to think through. Help me ask the right questions.",
            },
          ].map((suggestion) => (
            <Button
              key={suggestion.label}
              variant="ghost"
              size="sm"
              disabled={busy}
              onClick={() => {
                setText(suggestion.text);
                input.current?.focus();
              }}
            >
              {suggestion.label}
            </Button>
          ))}
        </div>
      )}
    </div>
  );
}
