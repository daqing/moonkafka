// Version negotiation tests (whitebox so BrokerVersions can be
// constructed directly and pick_common exercised in isolation).

///|
test "pick_common chooses the highest common version" {
  // (driver min, driver max, want, broker min, broker max, expected)
  let cases : Array[(Int, Int, Int, Int, Int, Int?)] = [
    (12, 16, 16, 0, 20, Some(16)), // broker covers everything: driver ceiling
    (12, 16, 16, 13, 20, Some(16)), // broker floor above driver floor
    (12, 16, 16, 0, 13, Some(13)), // broker max caps below the driver ceiling
    (12, 16, 13, 0, 20, Some(13)), // want caps below the driver ceiling
    (12, 16, 16, 0, 11, None), // broker too old: no overlap
    (12, 16, 16, 17, 20, None), // broker floor beyond the driver ceiling
    (12, 16, 11, 0, 20, None), // want below the driver floor
  ]
  for case in cases {
    let (dmin, dmax, want, bmin, bmax, expected) = case
    assert_eq(pick_common(dmin, dmax, want, bmin, bmax), expected)
  }
}

///|
test "driver matrix rows are well-formed" {
  let keys = [
    API_PRODUCE,
    API_FETCH,
    API_LIST_OFFSETS,
    API_METADATA,
    API_API_VERSIONS,
    API_SASL_HANDSHAKE,
    API_SASL_AUTHENTICATE,
  ]
  for key in keys {
    let row = driver_api_range(key).unwrap()
    assert_eq(row.key, key)
    assert_true(row.min <= row.max)
    assert_true(row.name.length() > 0)
  }
  assert_true(driver_api_range(9999) is None)
}

///|
fn versions_of(pairs : Array[(Int, Int, Int)]) -> BrokerVersions {
  let ranges : Map[Int, (Int, Int)] = Map([])
  for pair in pairs {
    let (key, min, max) = pair
    ranges[key] = (min, max)
  }
  { ranges, }
}

///|
test "pick caps at the driver ceiling and exposes ranges" {
  let v = versions_of([(API_PRODUCE, 0, 20), (API_FETCH, 11, 20)])
  // A broker newer than the driver still gets the driver's ceiling.
  assert_eq(v.pick(API_PRODUCE), 11)
  assert_eq(v.pick(API_FETCH), 12)
  assert_eq(v.range(API_PRODUCE), Some((0, 20)))
  assert_eq(v.range(75), None)
}

///|
test "pick raises when the broker does not advertise the API" {
  let v = versions_of([])
  let mut msg = ""
  ignore(v.pick(API_METADATA)) catch {
    ProtocolError::ProtocolError(m) => msg = m
    e => fail("expected ProtocolError, got \{e}")
  }
  assert_true(msg.contains("does not advertise API key 3 (Metadata)"))
}

///|
test "pick raises when no version overlaps" {
  // A pre-4.0 broker: Metadata tops out below the implemented floor.
  let v = versions_of([(API_METADATA, 0, 11)])
  let mut msg = ""
  ignore(v.pick(API_METADATA)) catch {
    ProtocolError::ProtocolError(m) => msg = m
    e => fail("expected ProtocolError, got \{e}")
  }
  assert_true(msg.contains("no common Metadata version"))
  assert_true(msg.contains("broker supports 0-11"))
  assert_true(msg.contains("moonkafka implements 12-13"))
}

///|
test "pick raises for APIs without codecs and wants below the floor" {
  let v = versions_of([(API_PRODUCE, 0, 20), (9999, 0, 20)])
  let mut missing = ""
  ignore(v.pick(9999)) catch {
    ProtocolError::ProtocolError(m) => missing = m
    e => fail("expected ProtocolError, got \{e}")
  }
  assert_true(missing.contains("implements no version of API key 9999"))
  let mut below_floor = ""
  ignore(v.pick(API_PRODUCE, want=Some(10))) catch {
    ProtocolError::ProtocolError(m) => below_floor = m
    e => fail("expected ProtocolError, got \{e}")
  }
  assert_true(below_floor.contains("no common Produce version"))
}
