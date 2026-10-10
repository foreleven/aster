import { BusinessRpcs } from "./rpcs/business.js";
import { RpcGroup } from "effect/rpc";
import { ContextRpcs } from "./rpcs/contexts.js";
import { GoalRpcs } from "./rpcs/goals.js";
import { TaskRpcs } from "./rpcs/tasks.js";
import { ApprovalRpcs } from "./rpcs/approvals.js";
import { ProcessingRpcs } from "./rpcs/processing.js";
import { RuntimeRpcs } from "./rpcs/runtime.js";
import { NotificationRpcs } from "./rpcs/notifications.js";

export const ApplicationRpcs = RpcGroup.make().merge(
  BusinessRpcs,
  ContextRpcs,
  GoalRpcs,
  TaskRpcs,
  ApprovalRpcs,
  ProcessingRpcs,
  RuntimeRpcs,
  NotificationRpcs,
);
