/** Callers validate TaskPath before selecting an execution owner. */
export const taskActorPath = (path: string) => `/user${path}`;
