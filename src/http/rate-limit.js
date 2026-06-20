/**
 * Create a small fixed-window rate limiter keyed by caller identity.
 *
 * @param {{ max: number, windowMs: number, now?: () => number }} options
 */
export function createFixedWindowRateLimiter(options) {
  const now = options.now ?? Date.now
  const windows = new Map()

  return (key) => {
    const currentTime = now()
    pruneExpiredWindows(windows, currentTime, options.windowMs)

    const existing = windows.get(key)
    const resetAt = existing && existing.resetAt > currentTime
      ? existing.resetAt
      : currentTime + options.windowMs
    const count = existing && existing.resetAt > currentTime ? existing.count : 0

    if (count >= options.max) {
      return {
        allowed: false,
        remaining: 0,
        resetAt,
        retryAfterSeconds: Math.max(1, Math.ceil((resetAt - currentTime) / 1000))
      }
    }

    const nextCount = count + 1
    windows.set(key, { count: nextCount, resetAt })
    return {
      allowed: true,
      remaining: Math.max(0, options.max - nextCount),
      resetAt,
      retryAfterSeconds: 0
    }
  }
}

function pruneExpiredWindows(windows, currentTime, windowMs) {
  if (windows.size === 0) return
  for (const [key, entry] of windows.entries()) {
    if (entry.resetAt <= currentTime - windowMs || entry.resetAt <= currentTime) {
      windows.delete(key)
    }
  }
}
