import { acquireActorStoreLock } from "../../src/storage/actor-store-lock.js";
try {
  const release = acquireActorStoreLock(process.argv[2]);
  process.send?.("acquired");
  process.once("message", () => {
    release();
    process.disconnect();
  });
} catch {
  process.send?.("rejected");
  process.disconnect();
}
