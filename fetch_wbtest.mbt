// Whitebox fetch tests: state manipulation the public API does not allow.

///|
test "fetch session epoch wrap forces a full request" {
  let s = FetchSession::new()
  s.handle_response(9, 0)
  let huge : Map[Int, (Int64, Int)] = Map([])
  huge[0] = (1L, 0)
  ignore(s.prepare("events", Uuid::zero(), huge, 100))
  // Pretend the epochs ran out.
  s.next_epoch = 0x7fffffff
  let wrapped = s.prepare("events", Uuid::zero(), huge, 100)
  assert_eq(wrapped.session_id, 0)
  assert_eq(wrapped.session_epoch, 0)
  assert_eq(wrapped.partitions.length(), 1)
}
