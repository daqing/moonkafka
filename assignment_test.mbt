// Assignment-strategy tests: pure placement checks over synthetic
// members and partition counts.

///|
test "range assignor splits each topic into contiguous chunks" {
  let counts : Map[String, Int] = Map([("events", 3)])
  let out = @moonkafka.compute_assignment(
    @moonkafka.RangeAssignor,
    [
      { member_id: "b", topics: ["events"], },
      { member_id: "a", topics: ["events"], },
    ],
    counts,
  )
  // Members sorted by id: "a" takes the extra partition in front.
  assert_eq(out.get("a").unwrap(), [("events", 0), ("events", 1)])
  assert_eq(out.get("b").unwrap(), [("events", 2)])
}

///|
test "round robin assignor circles sorted topic partitions" {
  let counts : Map[String, Int] = Map([("t1", 2), ("t2", 2)])
  let out = @moonkafka.compute_assignment(
    @moonkafka.RoundRobinAssignor,
    [
      { member_id: "a", topics: ["t1", "t2"], },
      { member_id: "b", topics: ["t1", "t2"], },
    ],
    counts,
  )
  assert_eq(out.get("a").unwrap(), [("t1", 0), ("t2", 0)])
  assert_eq(out.get("b").unwrap(), [("t1", 1), ("t2", 1)])
}

///|
test "sticky assignor keeps prior partitions and balances the spread" {
  let counts : Map[String, Int] = Map([("events", 2)])
  let owned : Map[String, Array[(String, Int)]] = Map([
    ("a", [("events", 0), ("events", 1)]),
  ])
  let out = @moonkafka.compute_assignment(
    @moonkafka.StickyAssignor,
    [
      { member_id: "a", topics: ["events"], },
      { member_id: "b", topics: ["events"], },
    ],
    counts,
    owned~,
  )
  // "a" keeps one partition, the mover is the newest one, "b" takes it.
  assert_eq(out.get("a").unwrap(), [("events", 0)])
  assert_eq(out.get("b").unwrap(), [("events", 1)])
}

///|
test "sticky assignor keeps a balanced assignment untouched" {
  let counts : Map[String, Int] = Map([("events", 2)])
  let owned : Map[String, Array[(String, Int)]] = Map([
    ("a", [("events", 0)]),
    ("b", [("events", 1)]),
  ])
  let out = @moonkafka.compute_assignment(
    @moonkafka.StickyAssignor,
    [
      { member_id: "a", topics: ["events"], },
      { member_id: "b", topics: ["events"], },
    ],
    counts,
    owned~,
  )
  assert_eq(out.get("a").unwrap(), [("events", 0)])
  assert_eq(out.get("b").unwrap(), [("events", 1)])
}
