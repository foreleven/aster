import { defineDoc } from "@earendil-works/pi-durable";

/** Pi admission and parent coverage survive the Actor handoff. */
export const DurableSteers = defineDoc<{
  inputs: { requestId: string; text: string; parent: string }[];
}>({
  kind: "app.aster.steers",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  initial: () => ({ inputs: [] }),
});
