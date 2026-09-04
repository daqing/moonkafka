// Blackbox tests for backoff scheduling and deadlines.

///|
test "backoff grows exponentially and caps" {
  assert_eq(@moonkafka.backoff_ms(100, 1000, 0), 100)
  assert_eq(@moonkafka.backoff_ms(100, 1000, 1), 200)
  assert_eq(@moonkafka.backoff_ms(100, 1000, 2), 400)
  assert_eq(@moonkafka.backoff_ms(100, 1000, 3), 800)
  assert_eq(@moonkafka.backoff_ms(100, 1000, 4), 1000)
  assert_eq(@moonkafka.backoff_ms(100, 1000, 10), 1000)
  assert_eq(@moonkafka.backoff_ms(100, 1000, 999), 1000)
  assert_eq(@moonkafka.backoff_ms(100, 1000, -5), 100)
}

///|
test "backoff next_ms jitters within plusminus 20 percent" {
  // each sample uses a fresh schedule so the base step is under test
  for _ in 0..<20 {
    let b = @moonkafka.Backoff::new()
    let ms = b.next_ms()
    assert_true(ms >= 80 && ms <= 120)
  }
  // advancing past the cap lands at 1000ms, still jittered
  let b = @moonkafka.Backoff::new()
  for _ in 0..<20 {
    ignore(b.next_ms())
  }
  let capped = b.next_ms()
  assert_true(capped >= 800 && capped <= 1200)
  b.reset()
  let fresh = b.next_ms()
  assert_true(fresh >= 80 && fresh <= 120)
}

///|
test "backoff new validates its range" {
  assert_true(raises(fn() raise { ignore(@moonkafka.Backoff::new(base_ms=0)) }))
  assert_true(
    raises(fn() raise {
      ignore(@moonkafka.Backoff::new(base_ms=2000, max_ms=1000))
    }),
  )
}

///|
test "deadline expiry math" {
  let d = @moonkafka.Deadline::after_ms(5000L)
  assert_false(d.expired())
  assert_true(d.remaining_ms() > 4000L)
  assert_true(d.remaining_ms() <= 5000L)
  let past = @moonkafka.Deadline::after_ms(-10_000L)
  assert_true(past.expired())
  assert_true(past.remaining_ms() < 0L)
}
