/** Business dates never depend on the host timezone. */
export const IM_TIME_ZONE = "Asia/Shanghai";
const formatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: IM_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});
export const imDate = (time: string | number): string => {
  const date = new Date(time);
  if (!Number.isFinite(date.getTime())) throw new Error(`Invalid IM timestamp: ${time}`);
  return formatter.format(date);
};
export const imDayStart = (date: string): number => {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error(`Invalid IM date: ${date}`);
  const time = Date.parse(`${date}T00:00:00+08:00`);
  if (!Number.isFinite(time) || imDate(time) !== date) throw new Error(`Invalid IM date: ${date}`);
  return time;
};
export const nextImDay = (date: string) => imDate(imDayStart(date) + 86_400_000);
