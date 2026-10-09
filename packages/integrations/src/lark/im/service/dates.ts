/** Business dates never depend on the host timezone. */
export const DEFAULT_TIME_ZONE = "Asia/Shanghai";
const formatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: DEFAULT_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});
export const formatDate = (time: string | number): string => {
  const date = new Date(time);
  if (!Number.isFinite(date.getTime())) throw new Error(`Invalid timestamp: ${time}`);
  return formatter.format(date);
};
export const dayStart = (date: string): number => {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error(`Invalid calendar date: ${date}`);
  const time = Date.parse(`${date}T00:00:00+08:00`);
  if (!Number.isFinite(time) || formatDate(time) !== date)
    throw new Error(`Invalid calendar date: ${date}`);
  return time;
};
export const nextDay = (date: string) => formatDate(dayStart(date) + 86_400_000);
