// Read-only quota coordination. Never use this for writes or provider actions.
// Keep authoritative reads fresh: no caching, including for post-write verification.
type Dependencies = { now: () => number; sleep: (ms: number) => Promise<void>; jitter: () => number };
const WINDOW_MS = 60_000;
const MAX_READS = 45;
const MAX_WAIT_MS = 130_000;
function errorStatus(error: unknown): number {
  const value = error as { response?: { status?: unknown }; status?: unknown; code?: unknown } | null;
  return Number(value?.response?.status ?? value?.status ?? value?.code);
}

export function createSheetsReadQuota({
  now = Date.now,
  sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms)),
  jitter = () => Math.random() * 1_000,
}: Partial<Dependencies> = {}): <T>(read: () => Promise<T>) => Promise<T> {
  const dispatches: number[] = [];
  let cooldownUntil = 0;

  return async function readWithQuota<T>(read: () => Promise<T>): Promise<T> {
    const deadline = now() + MAX_WAIT_MS;
    let lastQuotaError: unknown;
    for (let attempt = 0; attempt < 2; attempt++) {
      for (;;) {
        const current = now();
        while (dispatches.length && dispatches[0] + WINDOW_MS <= current) dispatches.shift();
        const next = Math.max(cooldownUntil, dispatches.length >= MAX_READS ? dispatches[0] + WINDOW_MS : 0);
        if (next <= current) { dispatches.push(current); break; }
        if (next > deadline) throw lastQuotaError ?? new Error("Google Sheets read quota wait exceeded; fresh verification deferred");
        await sleep(next - current);
      }
      try { return await read(); }
      catch (error) {
        if (errorStatus(error) !== 429) throw error;
        lastQuotaError = error;
        cooldownUntil = Math.max(cooldownUntil, now() + WINDOW_MS + jitter());
      }
    }
    throw lastQuotaError;
  };
}

export const readWithSheetsQuota = createSheetsReadQuota();
