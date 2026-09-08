// Blackbox tests for bootstrap server rotation.

///|
test "bootstrap servers rotate in round-robin order" {
  let servers = @moonkafka.BootstrapServers::new([
    { host: "a", port: 1, },
    { host: "b", port: 2, },
    { host: "c", port: 3, },
  ])
  assert_eq(servers.next().host, "a")
  assert_eq(servers.next().host, "b")
  assert_eq(servers.next().host, "c")
  assert_eq(servers.next().host, "a")
}
