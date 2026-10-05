import type { DescriptionPolicy } from "../reasoning/context-description.js";
import { taskView } from "../tasks/view.js";

export const coreDescriptions: readonly DescriptionPolicy[] = [
  {
    matches: (path) => taskView.matches!(path),
    identity: "Work executed by an internal or external agent",
  },
  { matches: (path) => /^\/goals\/[^/]+$/.test(path), identity: "Ongoing work goal" },
  {
    matches: (path) => /^\/signals\/[^/]+$/.test(path),
    identity: "Condition and schedule monitoring",
  },
];
