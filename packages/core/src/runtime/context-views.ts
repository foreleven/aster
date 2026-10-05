import { goalView, goalsRootView } from "../goals/view.js";
import { signalView, signalsRootView } from "../signals/view.js";
import { runView, tasksRootView } from "../tasks/view.js";
import { delegationView } from "../delegation/view.js";
import { approvalView } from "../approvals/view.js";

/** Installed before owners start, including views of dormant persisted owners. */
export const coreContextViews = [
  goalView,
  runView,
  delegationView,
  signalView,
  approvalView,
  goalsRootView,
  signalsRootView,
  tasksRootView,
];
