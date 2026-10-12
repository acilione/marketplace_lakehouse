export const dateTime = (value: string) =>
  new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "UTC",
  }).format(new Date(value));

/** Compact presentation only; searches, accessible labels and exports keep the full ID. */
export function displayIdentifier(value: string): string {
  return value.replace(/^([A-Z]+)-([a-f0-9]{8})[a-f0-9]{24}-(\d+)$/, "$1-$2-$3");
}
