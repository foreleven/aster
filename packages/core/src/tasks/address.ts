import { childActorName } from "../context/actor.js";

/** The caller decodes RunPath before addressing its existing domain owner. */
export const runActorPath = (path: string) => {
  const parts = path.split("/");
  if (parts[1] === "runs") return `/user${path}`;
  return `/user/${parts[1]}/${parts[2]}/${childActorName(`runs/${parts[4]}`)}`;
};
