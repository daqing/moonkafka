// Blackbox tests for the broker error-code table. Spot values were validated
// against the Kafka 4.3 protocol page and Errors.java when the table was
// generated.

///|
test "error table covers every 4.3 code with known names" {
  assert_eq(@moonkafka.error_name(-1), "UNKNOWN_SERVER_ERROR")
  assert_eq(@moonkafka.error_name(0), "NONE")
  assert_eq(@moonkafka.error_name(1), "OFFSET_OUT_OF_RANGE")
  assert_eq(@moonkafka.error_name(25), "UNKNOWN_MEMBER_ID")
  assert_eq(@moonkafka.error_name(133), "SHARE_SESSION_LIMIT_REACHED")
  assert_eq(@moonkafka.error_name(999), "UNKNOWN(999)")
  // Codes 0..133 are the published 4.3 range; none may fall back.
  for c in 0..<134 {
    assert_true(@moonkafka.error_name(c) != "UNKNOWN(\{c})")
  }
}

///|
test "retriable flags match the protocol table" {
  assert_false(@moonkafka.error_retriable(0)) // NONE
  assert_false(@moonkafka.error_retriable(1)) // OFFSET_OUT_OF_RANGE
  assert_true(@moonkafka.error_retriable(3)) // UNKNOWN_TOPIC_OR_PARTITION
  assert_true(@moonkafka.error_retriable(6)) // NOT_LEADER_OR_FOLLOWER
  assert_true(@moonkafka.error_retriable(13)) // NETWORK_EXCEPTION
  assert_false(@moonkafka.error_retriable(25)) // UNKNOWN_MEMBER_ID
  assert_true(@moonkafka.error_retriable(70)) // FETCH_SESSION_ID_NOT_FOUND
  assert_false(@moonkafka.error_retriable(999))
}

///|
test "BrokerError carries code metadata" {
  let e : @moonkafka.BrokerError = @moonkafka.BrokerError(
    1, "ListOffsets failed for partition 2",
  )
  assert_eq(e.name(), "OFFSET_OUT_OF_RANGE")
  assert_false(e.is_retriable())
  let re : @moonkafka.BrokerError = @moonkafka.BrokerError(
    13, "Fetch to node 3",
  )
  assert_eq(re.name(), "NETWORK_EXCEPTION")
  assert_true(re.is_retriable())
}
