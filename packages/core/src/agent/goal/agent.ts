import { instructions } from "../runtime/instructions.js";
import { ScreeningAgent } from "../screening/agent.js";
import { inputContent } from "./input.js";
import { logGoalResponse } from "./extensions.js";
import { ContextDirectory } from "../../config/context-directory.js";
import { AgentSession } from "../runtime/session.js";
import { AgentRuntime } from "../runtime/ordinary-runtime.js";
import { answerText } from "../../services/agent-input.js";
import { AgentError } from "../index.js";
import { goalTools } from "./tools/catalogue.js";
import { ToolInvocation } from "../shared/invocation.js";
import { CurrentContextPath } from "../../context/location.js";
import { DurableContext } from "../../context/store.js";
import { ExternalAgents } from "../../tasks/execution/contracts.js";
import { GoalState } from "../../goals/state/model.js";
import type { GoalIntent } from "../../goals/screening/intent.js";
import type { ResolvedGoalInput } from "../../goals/state/inputs.js";
import type { GoalDefinition } from "../../goals/contracts.js";
import { GoalSettings } from "../../goals/settings.js";
import { LanguageSettings, outputLanguageInstruction } from "../../config/language.js";
import { Effect, Context, Layer } from "effect";

import type { CurrentActors } from "../../services/actors.js";

export interface GoalConversation {
  readonly goal: GoalDefinition;
  readonly input: ResolvedGoalInput;
}

/** Model execution consumes resolved input and injected business capabilities; it never appends public replies. */
export class GoalAgent extends Context.Service<
  GoalAgent,
  {
    readonly converse: (
      options: GoalConversation,
    ) => Effect.Effect<string, AgentError, CurrentActors | GoalState | CurrentContextPath>;
    readonly screen: (
      goal: GoalDefinition,
      intent: GoalIntent,
    ) => Effect.Effect<{ relevant: boolean; reason: string }, AgentError>;
  }
>()("goals/Agent") {
  static readonly make = Effect.fn("GoalAgent.make")(function* (): Effect.fn.Return<
    GoalAgent["Service"],
    never,
    | AgentRuntime
    | AgentSession
    | GoalSettings
    | ContextDirectory
    | LanguageSettings
    | ExternalAgents
    | DurableContext
  > {
    const harness = yield* AgentSession;
    const settings = yield* GoalSettings;
    const contexts = yield* ContextDirectory;
    const { outputLanguage: language } = yield* LanguageSettings;
    const screening = yield* ScreeningAgent.make(settings.reasoning.model, language);
    const executors = yield* ExternalAgents;
    const registry = yield* DurableContext;
    return GoalAgent.of({
      screen: (goal, intent) => screening.screen(goal, intent),
      converse: Effect.fn("Goal.conversation")(function* (options) {
        const { goal, input } = options;
        const source = yield* CurrentContextPath;
        return yield* harness
          .withConversation(
            {
              name: settings.reasoning.model,
              tools: goalTools(),
              onResponse: (message) =>
                logGoalResponse(message, {
                  goalPath: `/goals/${goal.slug}`,
                  phase: "conversation",
                  inputId: input.inputId,
                }),
              owner: source,
              workspaceId: `/goals/${goal.slug}`,
              extensionName: `aster-goal-tools:${goal.slug}`,
              instructions: `${instructions.goal}

## Output language
${outputLanguageInstruction(language)}

## Current Goal
${goal.title ?? goal.slug}
${goal.description}

## Configured Contexts
This directory contains configured source paths and descriptions, not retrieved evidence or a guarantee of connection readiness. For requests needing fresh information, start an internal Agent Task and include known source paths using contextPaths and the user constraints. Path hints are optional and do not prove source readiness. The Task discovers supported commands and retrieves evidence; do not claim a source is inaccessible without checking through a Task. Memory retrieval uses the Task's memory tools. Descriptions identify sources and do not grant authority for external actions.
${JSON.stringify(contexts, null, 2)}`,
              contextBudget: {
                contextTokens: settings.reasoning.contextTokens,
                reserveTokens: settings.reasoning.reserveTokens,
              },
            },
            (conversation) =>
              Effect.gen(function* () {
                const submission = yield* conversation.submit({
                  requestId: input.inputId,
                  content: inputContent(input),
                });
                return answerText(yield* submission.wait);
              }),
          )
          .pipe(
            Effect.provideService(ToolInvocation, {
              ownerPath: source,
              inputId: input.inputId,
              acceptedAt: input.receivedAt,
              remainingAgentTurns: Math.max(0, input.remainingAgentTurns - 1),
            }),
            Effect.provideService(ExternalAgents, executors),
            Effect.provideService(DurableContext, registry),
          );
      }),
    });
  });
  static readonly layer = Layer.effect(GoalAgent, GoalAgent.make());
}
