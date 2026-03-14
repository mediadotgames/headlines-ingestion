export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function isRetryableStatus(status: number): boolean {
  return (
    status === 429 ||
    status === 500 ||
    status === 502 ||
    status === 503 ||
    status === 504
  );
}

export function isRetryableError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);

  return (
    msg.includes("fetch failed") ||
    msg.includes("ECONNRESET") ||
    msg.includes("ETIMEDOUT") ||
    msg.includes("EAI_AGAIN") ||
    msg.includes("429") ||
    msg.includes("500") ||
    msg.includes("502") ||
    msg.includes("503") ||
    msg.includes("504")
  );
}

export function backoffDelayMs(
  attempt: number,
  baseMs = 1000,
  capMs = 15000,
  jitterMaxMs = 500,
): number {
  const backoff = Math.min(baseMs * 2 ** (attempt - 1), capMs);
  const jitter = Math.floor(Math.random() * jitterMaxMs);
  return backoff + jitter;
}
