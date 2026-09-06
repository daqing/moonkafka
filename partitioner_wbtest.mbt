// Whitebox tests for partition routing: the sticky cache, the round-robin
// cursor, murmur2 keyed placement, and the manual override. These reach
// into PartitionRouter/StickyPartitioner directly, without a connection.

///|
fn router_raises(f : () -> Int raise) -> Bool {
  let _ = f() catch { _ => return true }
  false
}

///|
test "sticky partitioner holds one partition until a batch boundary" {
  let s = StickyPartitioner::new(0xDEADBEEFCAFEBABEUL)
  let first = s.partition(5)
  assert_true(first >= 0 && first < 5)
  assert_eq(s.partition(5), first)
  assert_eq(s.partition(5), first)
  s.on_new_batch(5)
  let second = s.partition(5)
  assert_true(second != first)
  assert_eq(s.partition(5), second)
}

///|
test "sticky boundary switch never repeats the previous partition" {
  let s = StickyPartitioner::new(42UL)
  for _ in 0..<100 {
    let before = s.partition(7)
    s.on_new_batch(7)
    assert_true(s.partition(7) != before)
  }
}

///|
test "sticky boundary switches wrap around all partitions" {
  let s = StickyPartitioner::new(1UL)
  let first = s.partition(4)
  let seen : Array[Bool] = Array::make(4, false)
  for _ in 0..<4 {
    seen[s.partition(4)] = true
    s.on_new_batch(4)
  }
  // four consecutive boundaries from any start visit every partition
  for i in 0..<4 {
    assert_true(seen[i])
  }
  assert_eq(s.partition(4), first)
}

///|
test "sticky partitioner with one partition always returns zero" {
  let s = StickyPartitioner::new(3UL)
  assert_eq(s.partition(1), 0)
  s.on_new_batch(1)
  assert_eq(s.partition(1), 0)
}

///|
test "sticky partitioner reseeds when the partition count changes" {
  let s = StickyPartitioner::new(7UL)
  ignore(s.partition(3))
  // metadata refresh shrinks/grows the topic: the stale index must not
  // leak out of range
  let after = s.partition(9)
  assert_true(after >= 0 && after < 9)
}

///|
test "round robin strategy cycles every partition, keys ignored" {
  let r = PartitionRouter::new(RoundRobin, 1UL)
  let key = @utf8.encode("ignored")
  let seen : Array[Bool] = Array::make(4, false)
  for _ in 0..<4 {
    seen[r.pick(4, Some(key), None)] = true
  }
  for i in 0..<4 {
    assert_true(seen[i])
  }
}

///|
test "murmur2 strategy places keyed records by hash, unkeyed sticky" {
  let key = @utf8.encode("order-123")
  let expected = (@internal.murmur2(key) & 0x7fffffff) % 6
  let r = PartitionRouter::new(Murmur2, 1UL)
  // keyed placement is stable across calls (and identical to other clients)
  for _ in 0..<5 {
    assert_eq(r.pick(6, Some(key), None), expected)
  }
  // unkeyed records stick per batch: one record per send until the
  // accumulator lands, so consecutive picks advance (never repeat)
  let a = r.pick(6, None, None)
  let b = r.pick(6, None, None)
  assert_true(a != b)
}

///|
test "murmur2 round robin strategy hashes keys, spreads unkeyed" {
  let key = @utf8.encode("k")
  let r = PartitionRouter::new(Murmur2RoundRobin, 1UL)
  assert_eq(
    r.pick(3, Some(key), None),
    (@internal.murmur2(key) & 0x7fffffff) % 3,
  )
  let seen : Array[Bool] = Array::make(3, false)
  for _ in 0..<3 {
    seen[r.pick(3, None, None)] = true
  }
  for i in 0..<3 {
    assert_true(seen[i])
  }
}

///|
test "explicit partition override wins and validates the range" {
  let r = PartitionRouter::new(Murmur2, 1UL)
  let key = @utf8.encode("k")
  assert_eq(r.pick(4, None, Some(2)), 2)
  assert_eq(r.pick(4, Some(key), Some(0)), 0)
  assert_eq(r.pick(4, Some(key), Some(3)), 3)
  assert_true(router_raises(fn() raise { r.pick(4, None, Some(4)) }))
  assert_true(router_raises(fn() raise { r.pick(4, None, Some(-1)) }))
}

///|
test "single-partition topics always route to zero" {
  for strategy in [Murmur2, Murmur2RoundRobin, RoundRobin] {
    let r = PartitionRouter::new(strategy, 1UL)
    assert_eq(r.pick(1, None, None), 0)
    assert_eq(r.pick(1, Some(b"k"), None), 0)
  }
}
