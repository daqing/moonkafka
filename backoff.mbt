// Retry backoff scheduling and deadlines, built on the @async wall clock.
// Growth is a pure, testable function; jitter comes from wall-clock low
// bits, which is fine for retry pacing (it is not security-sensitive).

///|
/// Pure exponential growth: base_ms shifted by `attempt`, capped at max_ms
/// (and at a shift ceiling so large attempts cannot overflow).
pub fn backoff_ms(base_ms : Int, max_ms : Int, attempt : Int) -> Int {
  let attempt = if attempt < 0 {
    0
  } else if attempt > 16 {
    16
  } else {
    attempt
  }
  let raw = base_ms << attempt
  if raw > max_ms || raw <= 0 {
    max_ms
  } else {
    raw
  }
}

///|
/// Stateful backoff schedule: next_ms() grows exponentially per call and
/// applies ±20% wall-clock jitter so retried clients do not synchronize.
pub struct Backoff {
  base_ms : Int
  max_ms : Int
  mut attempt : Int
} derive(@debug.Debug)

///|
pub fn Backoff::new(
  base_ms? : Int = 100,
  max_ms? : Int = 1000,
) -> Backoff raise {
  if base_ms <= 0 || max_ms < base_ms {
    raise ProtocolError::ProtocolError(
      "backoff needs base_ms > 0 and max_ms >= base_ms, got \{base_ms}/\{max_ms}",
    )
  }
  { base_ms, max_ms, attempt: 0, }
}

///|
/// Next delay in milliseconds, advancing the attempt counter.
pub fn Backoff::next_ms(self : Backoff) -> Int {
  let ms = backoff_ms(self.base_ms, self.max_ms, self.attempt)
  self.attempt = self.attempt + 1
  let jitter = 80 + (@async.now() % 41L).to_int() // 80..120 percent
  if ms > 16_000_000 {
    ms // too large for the multiplication; skip jitter
  } else {
    let jittered = ms * jitter / 100
    if jittered < 1 {
      1
    } else {
      jittered
    }
  }
}

///|
pub fn Backoff::reset(self : Backoff) -> Unit {
  self.attempt = 0
}

///|
/// A point in time measured against the @async wall clock.
pub struct Deadline {
  at_ms : Int64
} derive(@debug.Debug)

///|
pub fn Deadline::after_ms(ms : Int64) -> Deadline {
  { at_ms: @async.now() + ms, }
}

///|
pub fn Deadline::expired(self : Deadline) -> Bool {
  @async.now() >= self.at_ms
}

///|
/// Milliseconds until expiry; negative once past.
pub fn Deadline::remaining_ms(self : Deadline) -> Int64 {
  self.at_ms - @async.now()
}
