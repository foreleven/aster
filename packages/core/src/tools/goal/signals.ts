import { Effect, Schema } from "effect";
import {
  ControlSignalInput,
  CreateSignalInput,
  UpdateSignalInput,
} from "../../signals/contracts.js";
import {
  changeSignal,
  listOwnedSignals,
  ownedSignalPath,
  signalDefinition,
} from "../../signals/operations.js";
import { type SignalChangeInput } from "../../signals/protocol.js";
import { defineTool } from "../define.js";
import { ToolInvocation, toolRequestId } from "../invocation.js";

const change = Effect.fn("SignalTool.change")(function* (
  id: string,
  command: SignalChangeInput["change"],
  callId: string,
) {
  const invocation = yield* ToolInvocation;
  return yield* changeSignal({
    source: invocation.ownerPath,
    requestId: yield* toolRequestId(callId),
    remainingAgentTurns: invocation.remainingAgentTurns,
    target: ownedSignalPath(invocation.ownerPath, id),
    change: command,
  });
});
export const signalList = () =>
  defineTool(
    {
      name: "signal_list",
      replay: "safe",
      label: "Read Signals",
      description:
        "Read the current Goal's rules, actions, statuses and observed definition versions.",
      parameters: Schema.Struct({}),
    },
    () => ToolInvocation.pipe(Effect.flatMap(({ ownerPath }) => listOwnedSignals(ownerPath))),
  );
export const createSignal = () =>
  defineTool(
    {
      name: "create_signal",
      replay: "never",
      label: "Create Signal",
      description:
        "Create a rule belonging to the current Goal. Select a Context condition or timezone-qualified schedule and an explicit Task or Goal-message action. Internal Tasks can discover and query connected Contexts. Feedback returns to this Goal.",
      parameters: CreateSignalInput,
    },
    (input, callId) =>
      ToolInvocation.pipe(
        Effect.flatMap(({ ownerPath }) =>
          change(
            input.id,
            {
              operation: "create",
              definition: signalDefinition(input, ownerPath),
            },
            callId,
          ),
        ),
      ),
  );
export const updateSignal = () =>
  defineTool(
    {
      name: "update_signal",
      replay: "never",
      label: "Update Signal",
      description:
        "Replace the current Goal's rule using its observed positive definition version. Existing frozen occurrences keep their original action and evidence.",
      parameters: UpdateSignalInput,
    },
    (input, callId) =>
      ToolInvocation.pipe(
        Effect.flatMap(({ ownerPath }) =>
          change(
            input.id,
            {
              operation: "update",
              version: input.version,
              definition: signalDefinition(input, ownerPath),
            },
            callId,
          ),
        ),
      ),
  );
export const controlSignal = () =>
  defineTool(
    {
      name: "control_signal",
      replay: "never",
      label: "Control Signal",
      description:
        "Pause, resume or delete a rule belonging to the current Goal using its observed version. Deletion cancels only occurrences that have not started; it does not cancel submitted Tasks.",
      parameters: ControlSignalInput,
    },
    ({ id, version, operation }, callId) => change(id, { operation, version }, callId),
  );
