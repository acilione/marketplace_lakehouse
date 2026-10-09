import { CAUSES, OWNERS, STATUSES, type WorkStore } from "./model";

export const STORAGE_KEY = "marketplace-control-tower:demo:v1";
export interface DemoState {
  version: 1;
  batch: number;
  work: WorkStore;
}
export const initialState = (): DemoState => ({
  version: 1,
  batch: 0,
  work: {},
});
/** Local storage is untrusted input; reject incompatible or malformed workspaces. */
export function parseState(value: string | null): DemoState {
  if (value === null) return initialState();
  const state: unknown = JSON.parse(value);
  if (
    !isObject(state) ||
    state.version !== 1 ||
    ![0, 1].includes(Number(state.batch)) ||
    typeof state.batch !== "number" ||
    !isObject(state.work)
  )
    throw new Error("Invalid workspace format.");
  for (const [id, work] of Object.entries(state.work)) {
    if (
      !/^ORD-\d+:(missing_capture|amount_mismatch|currency_mismatch)$/.test(
        id,
      ) ||
      !isObject(work) ||
      !STATUSES.some((status) => status === work.status) ||
      !OWNERS.some((owner) => owner === work.owner) ||
      !(
        work.cause === null ||
        (typeof work.cause === "string" && Object.hasOwn(CAUSES, work.cause))
      ) ||
      !Number.isSafeInteger(work.revision) ||
      !Array.isArray(work.history) ||
      work.revision !== work.history.length ||
      (work.status === "resolved"
        ? work.cause === null || work.owner === "Unassigned"
        : work.cause !== null)
    )
      throw new Error("Invalid case record.");
    for (const entry of work.history) {
      if (
        !isObject(entry) ||
        typeof entry.id !== "string" ||
        typeof entry.at !== "string" ||
        !Number.isFinite(Date.parse(entry.at)) ||
        entry.actor !== "Demo operator" ||
        typeof entry.message !== "string" ||
        typeof entry.note !== "string" ||
        entry.note.length > 1000
      )
        throw new Error("Invalid case history.");
    }
  }
  return state as unknown as DemoState;
}
function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
