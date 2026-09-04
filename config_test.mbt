// Blackbox tests for configuration parsing and validation.

///|
/// True when the raising expression raises.
fn raises(f : () -> Unit raise) -> Bool {
  f() catch {
    _ => return true
  }
  false
}

///|
test "host port parsing" {
  assert_eq(@moonkafka.HostPort::parse("broker.example.com:9093"), {
    host: "broker.example.com",
    port: 9093,
  })
  assert_eq(@moonkafka.HostPort::parse("localhost"), {
    host: "localhost",
    port: 9092,
  })
  assert_eq(@moonkafka.HostPort::parse("[::1]:9092"), {
    host: "::1",
    port: 9092,
  })
  assert_eq(@moonkafka.HostPort::parse("[::1]"), { host: "::1", port: 9092, })
  assert_true(raises(fn() raise { ignore(@moonkafka.HostPort::parse("")) }))
  assert_true(raises(fn() raise { ignore(@moonkafka.HostPort::parse("[::1")) }))
  assert_true(
    raises(fn() raise { ignore(@moonkafka.HostPort::parse("[::1]junk")) }),
  )
  assert_true(
    raises(fn() raise { ignore(@moonkafka.HostPort::parse("host:port")) }),
  )
  assert_true(
    raises(fn() raise { ignore(@moonkafka.HostPort::parse("host:0")) }),
  )
  assert_true(
    raises(fn() raise { ignore(@moonkafka.HostPort::parse("host:99999")) }),
  )
  assert_true(
    raises(fn() raise { ignore(@moonkafka.HostPort::parse(":9092")) }),
  )
}

///|
test "common config defaults validate and parse bootstrap list" {
  let config = @moonkafka.CommonConfig::new(["a:9092", "b"])
  assert_eq(config.client_id, "moonkafka")
  assert_eq(config.request_timeout_ms, 30000)
  assert_eq(config.retry_backoff_ms, 100)
  assert_eq(config.retry_backoff_max_ms, 1000)
  assert_eq(config.bootstrap_addresses().length(), 2)
  assert_eq(config.bootstrap_addresses()[1].port, 9092)
}

///|
test "common config rejects invalid values" {
  assert_true(raises(fn() raise { ignore(@moonkafka.CommonConfig::new([])) }))
  assert_true(
    raises(fn() raise {
      ignore(@moonkafka.CommonConfig::new(["host:9092"], request_timeout_ms=0))
    }),
  )
  assert_true(
    raises(fn() raise {
      ignore(@moonkafka.CommonConfig::new(["no-such:99999"]))
    }),
  )
}

///|
test "producer config validates acks and topic" {
  let config = @moonkafka.ProducerConfig::new(["b:9092"], "events")
  assert_eq(config.acks, 1)
  assert_true(
    raises(fn() raise {
      ignore(@moonkafka.ProducerConfig::new(["b:9092"], "events", acks=0))
    }),
  )
  assert_true(
    raises(fn() raise { ignore(@moonkafka.ProducerConfig::new(["b:9092"], "")) }),
  )
}

///|
test "consumer config defaults to earliest" {
  let config = @moonkafka.ConsumerConfig::new(["b:9092"], "events")
  assert_true(config.start_from is Earliest)
}
