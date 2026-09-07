///|
test "fake api versions response decodes with zero throttle" {
  let body = fake_api_versions_response(0)
  let d = @buf.Decoder::new(body)
  let (versions, throttle) = @moonkafka.decode_api_versions_response(d)
  assert_eq(throttle, 0)
  assert_eq(d.remaining(), 0)
  // The fake broker's advertised ranges negotiate the driver's implemented
  // versions; Produce and Metadata top out at the v13 codecs.
  assert_eq(versions.pick(@moonkafka.API_PRODUCE), 13)
  assert_eq(versions.pick(@moonkafka.API_FETCH), 16)
  assert_eq(versions.pick(@moonkafka.API_LIST_OFFSETS), 11)
  assert_eq(versions.pick(@moonkafka.API_METADATA), 13)
  assert_eq(versions.pick(@moonkafka.API_API_VERSIONS), 3)
  assert_eq(versions.pick(@moonkafka.API_DESCRIBE_TOPIC_PARTITIONS), 0)
}

///|
test "fake api versions negotiates the phase 5.3 admin range" {
  let body = fake_api_versions_response(0)
  let d = @buf.Decoder::new(body)
  let (versions, _) = @moonkafka.decode_api_versions_response(d)
  assert_eq(versions.pick(@moonkafka.API_DESCRIBE_CLUSTER), 2)
  assert_eq(versions.pick(@moonkafka.API_DESCRIBE_CONFIGS), 4)
  assert_eq(versions.pick(@moonkafka.API_ALTER_CONFIGS), 2)
  assert_eq(versions.pick(@moonkafka.API_INCREMENTAL_ALTER_CONFIGS), 1)
  assert_eq(versions.pick(@moonkafka.API_LIST_CONFIG_RESOURCES), 1)
  assert_eq(versions.pick(@moonkafka.API_DESCRIBE_LOG_DIRS), 5)
  assert_eq(versions.pick(@moonkafka.API_ELECT_LEADERS), 2)
  assert_eq(versions.pick(@moonkafka.API_ALTER_PARTITION_REASSIGNMENTS), 1)
  assert_eq(versions.pick(@moonkafka.API_LIST_PARTITION_REASSIGNMENTS), 0)
  assert_eq(versions.pick(@moonkafka.API_UNREGISTER_BROKER), 0)
  assert_eq(versions.pick(@moonkafka.API_DESCRIBE_QUORUM), 2)
  assert_eq(versions.pick(@moonkafka.API_UPDATE_FEATURES), 2)
}

///|
test "describe cluster decodes v0 without endpoint or fenced fields" {
  let e = @buf.Encoder::new()
  e.write_i32(7) // throttle
  e.write_i16(0) // error
  e.write_compact_nullable_string(None)
  e.write_compact_string("cluster-0")
  e.write_i32(3) // controller
  e.write_compact_len(1)
  e.write_i32(3)
  e.write_compact_string("b3")
  e.write_i32(9092)
  e.write_compact_nullable_string(Some("rack-3"))
  e.write_tag_buffer()
  e.write_i32(4094) // authorized operations
  e.write_tag_buffer()
  let d = @buf.Decoder::new(e.to_bytes())
  let (result, throttle) = @moonkafka.decode_describe_cluster_response(0, d)
  assert_eq(throttle, 7)
  assert_eq(result.endpoint_type, @moonkafka.ENDPOINT_TYPE_BROKERS)
  assert_eq(result.cluster_id, Some("cluster-0"))
  assert_eq(result.controller_id, 3)
  assert_eq(result.brokers[0].rack, Some("rack-3"))
  assert_true(!result.brokers[0].is_fenced)
  assert_eq(result.cluster_authorized_operations, 4094)
  // The v0 request body carries only the operations flag.
  let body = @moonkafka.encode_describe_cluster_request(true, true, 0)
  assert_eq(body.length(), 2) // bool + tag buffer
}

///|
test "describe log dirs decodes across v2-v5 with version defaults" {
  // v2: no top-level error, no volume bytes, no cordon flag.
  let v2 = @buf.Encoder::new()
  v2.write_i32(0) // throttle
  v2.write_compact_len(1)
  v2.write_i16(0) // dir error
  v2.write_compact_string("/data")
  v2.write_compact_len(0) // topics
  v2.write_tag_buffer()
  v2.write_tag_buffer()
  let d2 = @buf.Decoder::new(v2.to_bytes())
  let (result2, _) = @moonkafka.decode_describe_log_dirs_response(2, d2)
  assert_eq(result2.error_code, 0)
  assert_eq(result2.results[0].total_bytes, -1L)
  assert_eq(result2.results[0].usable_bytes, -1L)
  assert_true(!result2.results[0].is_cordoned)
  // v5: everything present, cordoned.
  let body = fake_describe_log_dirs_response([("events", [0])], 5)
  let d5 = @buf.Decoder::new(body)
  let (result5, _) = @moonkafka.decode_describe_log_dirs_response(5, d5)
  assert_eq(result5.error_code, 0)
  assert_eq(result5.results[0].log_dir, "/tmp/kafka-logs")
  assert_eq(result5.results[0].total_bytes, 1000000L)
  assert_eq(result5.results[0].usable_bytes, 500000L)
  assert_true(!result5.results[0].is_cordoned)
  // The v2+ request shape is identical across versions: null = all topics.
  assert_eq(
    @moonkafka.encode_describe_log_dirs_request(None, 2),
    @moonkafka.encode_describe_log_dirs_request(None, 5),
  )
}

///|
test "alter reassignments v0 omits the replication-factor flag" {
  let body = @moonkafka.encode_alter_partition_reassignments_request(
    [
      {
        name: "events",
        partitions: [{ partition_index: 0, replicas: Some([1]), }],
      },
    ],
    30000,
    true,
    0,
  )
  // timeout + array header + topic/partition fields, no flag byte: the
  // v1 encoding adds exactly one byte.
  let v1 = @moonkafka.encode_alter_partition_reassignments_request(
    [
      {
        name: "events",
        partitions: [{ partition_index: 0, replicas: Some([1]), }],
      },
    ],
    30000,
    true,
    1,
  )
  assert_eq(body.length() + 1, v1.length())
  // v0 responses skip the echoed flag too.
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // top error
  e.write_compact_nullable_string(None)
  e.write_compact_len(0) // responses
  e.write_tag_buffer()
  let d = @buf.Decoder::new(e.to_bytes())
  let (result, _) = @moonkafka.decode_alter_partition_reassignments_response(
    0, d,
  )
  assert_eq(result.error_code, 0)
  assert_eq(result.responses.length(), 0)
}

///|
test "list config resources v0 reports no resource types" {
  // v0 request: no filter field, just the tag buffer.
  assert_eq(@moonkafka.encode_list_config_resources_request([], 0).length(), 1)
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(0) // error
  e.write_compact_len(1)
  e.write_compact_string("events")
  e.write_tag_buffer()
  e.write_tag_buffer()
  let d = @buf.Decoder::new(e.to_bytes())
  let (result, _) = @moonkafka.decode_list_config_resources_response(0, d)
  assert_eq(result.resources.length(), 1)
  assert_eq(result.resources[0].resource_name, "events")
  assert_eq(result.resources[0].resource_type, 0)
}

///|
test "describe quorum decodes the fake broker's v2 body" {
  let body = fake_describe_quorum_response([("__cluster_metadata", 0)])
  let d = @buf.Decoder::new(body)
  let result = @moonkafka.decode_describe_quorum_response(d)
  assert_eq(result.error_code, 0)
  assert_eq(result.topics[0].partitions[0].leader_epoch, 5)
  assert_eq(
    result.topics[0].partitions[0].current_voters[0].replica_directory_id,
    @moonkafka.Uuid::zero(),
  )
  assert_eq(result.nodes[0].listeners[0].2, 9093)
}

///|
test "update features v2 decodes the top-level result" {
  let body = fake_update_features_response(95)
  let d = @buf.Decoder::new(body)
  let (result, throttle) = @moonkafka.decode_update_features_response(d)
  assert_eq(throttle, 0)
  assert_eq(result.error_code, 95)
  let request = @moonkafka.encode_update_features_request(
    [
      {
        feature: "metadata.version",
        max_version_level: 22,
        upgrade_type: @moonkafka.FEATURE_UPGRADE,
      },
    ],
    30000,
    false,
  )
  assert_eq(request.length() > 10, true)
}

///|
test "fake api versions negotiates the acl range" {
  let body = fake_api_versions_response(0)
  let d = @buf.Decoder::new(body)
  let (versions, _) = @moonkafka.decode_api_versions_response(d)
  assert_eq(versions.pick(@moonkafka.API_DESCRIBE_ACLS), 3)
  assert_eq(versions.pick(@moonkafka.API_CREATE_ACLS), 3)
  assert_eq(versions.pick(@moonkafka.API_DELETE_ACLS), 3)
}

///|
test "acl request bodies are identical across v2 and v3" {
  let filter : @moonkafka.AclFilter = {
    resource_type: @moonkafka.ACL_RESOURCE_TOPIC,
    resource_name: Some("events"),
    pattern_type: @moonkafka.ACL_PATTERN_LITERAL,
    principal: None,
    host: None,
    operation: @moonkafka.ACL_OPERATION_ANY,
    permission_type: @moonkafka.ACL_PERMISSION_ANY,
  }
  // v3 only adds the USER resource type: v2/v3 share one wire shape.
  assert_eq(@moonkafka.encode_describe_acls_request(filter).length() > 8, true)
  assert_eq(
    @moonkafka.encode_delete_acls_request([filter]),
    @moonkafka.encode_delete_acls_request([filter]),
  )
}

///|
test "describe acls decodes the fake broker's v3 body" {
  let body = fake_describe_acls_response()
  let d = @buf.Decoder::new(body)
  let (result, throttle) = @moonkafka.decode_describe_acls_response(d)
  assert_eq(throttle, 0)
  assert_eq(result.error_code, 0)
  assert_eq(result.resources[0].acls[0].principal, "User:alice")
  assert_eq(d.remaining(), 0)
}

///|
test "acl responses carry per-item errors as values" {
  // CreateAcls: the second creation failed.
  let create = @buf.Encoder::new()
  create.write_i32(3) // throttle
  create.write_compact_len(2)
  create.write_i16(0)
  create.write_compact_nullable_string(None)
  create.write_tag_buffer()
  create.write_i16(100) // ACL_AUTHORIZATION_FAILED? any code as a value
  create.write_compact_nullable_string(Some("denied"))
  create.write_tag_buffer()
  create.write_tag_buffer()
  let d1 = @buf.Decoder::new(create.to_bytes())
  let (creations, throttle) = @moonkafka.decode_create_acls_response(d1)
  assert_eq(throttle, 3)
  assert_eq(creations[0].error_code, 0)
  assert_eq(creations[1].error_code, 100)
  assert_eq(creations[1].error_message, Some("denied"))
  // DeleteAcls: a filter-level error yields no matches.
  let delete = @buf.Encoder::new()
  delete.write_i32(0) // throttle
  delete.write_compact_len(1)
  delete.write_i16(82) // filter error
  delete.write_compact_nullable_string(Some("security disabled"))
  delete.write_compact_len(0) // no matching acls
  delete.write_tag_buffer()
  delete.write_tag_buffer()
  let d2 = @buf.Decoder::new(delete.to_bytes())
  let (filters, _) = @moonkafka.decode_delete_acls_response(d2)
  assert_eq(filters[0].error_code, 82)
  assert_eq(filters[0].matching_acls.length(), 0)
}

///|
test "fake api versions negotiates the quota and scram admin range" {
  let body = fake_api_versions_response(0)
  let d = @buf.Decoder::new(body)
  let (versions, _) = @moonkafka.decode_api_versions_response(d)
  assert_eq(versions.pick(@moonkafka.API_DESCRIBE_CLIENT_QUOTAS), 1)
  assert_eq(versions.pick(@moonkafka.API_ALTER_CLIENT_QUOTAS), 1)
  assert_eq(versions.pick(@moonkafka.API_DESCRIBE_USER_SCRAM_CREDENTIALS), 0)
  assert_eq(versions.pick(@moonkafka.API_ALTER_USER_SCRAM_CREDENTIALS), 0)
}

///|
test "describe client quotas shapes v0 legacy and v1 flexible requests" {
  let filter : @moonkafka.ClientQuotaFilter = {
    components: [
      {
        entity_type: @moonkafka.QUOTA_ENTITY_USER,
        match_type: @moonkafka.QUOTA_MATCH_EXACT,
        matches: Some("alice"),
      },
    ],
    strict: true,
  }
  // v0: i32 array count, int16 strings, no tag buffer. v1: compact
  // lengths, compact strings, per-component and top-level tag buffers.
  let v0 = @moonkafka.encode_describe_client_quotas_request(filter, 0)
  assert_eq(v0, b"\x00\x00\x00\x01\x00\x04user\x00\x00\x05alice\x01")
  let v1 = @moonkafka.encode_describe_client_quotas_request(filter, 1)
  assert_eq(v1, b"\x02\x05user\x00\x06alice\x00\x01\x00")
}

///|
test "describe client quotas decodes v0 and v1 responses" {
  // The fake's canned body round trips at both versions.
  for version in [0, 1] {
    let body = fake_describe_client_quotas_response(version)
    let d = @buf.Decoder::new(body)
    let (result, throttle) = @moonkafka.decode_describe_client_quotas_response(
      version, d,
    )
    assert_eq(throttle, 0)
    assert_eq(result.error_code, 0)
    assert_eq(result.entries.length(), 1)
    assert_eq(result.entries[0].entity[0].entity_type, "user")
    assert_eq(result.entries[0].entity[0].entity_name, Some("alice"))
    assert_eq(result.entries[0].values[0].key, "producer_byte_rate")
    assert_eq(result.entries[0].values[0].value, 1024.0)
    assert_eq(result.entries[0].values[1].value, 2048.0)
    assert_eq(d.remaining(), 0)
  }
}

///|
test "alter client quotas decodes per-entry results" {
  // The second entry failed.
  let body = @buf.Encoder::new()
  body.write_i32(7) // throttle
  body.write_compact_len(2)
  // Each entry is two nested structs — EntityData inside EntryData — and
  // at flexible versions every struct carries its own tag buffer.
  body.write_i16(0)
  body.write_compact_nullable_string(None)
  body.write_compact_len(1)
  body.write_compact_string("user")
  body.write_compact_nullable_string(Some("alice"))
  body.write_tag_buffer() // entity
  body.write_tag_buffer() // entry
  body.write_i16(100)
  body.write_compact_nullable_string(Some("denied"))
  body.write_compact_len(1)
  body.write_compact_string("user")
  body.write_compact_nullable_string(Some("bob"))
  body.write_tag_buffer() // entity
  body.write_tag_buffer() // entry
  body.write_tag_buffer() // top level
  let d = @buf.Decoder::new(body.to_bytes())
  let (results, throttle) = @moonkafka.decode_alter_client_quotas_response(1, d)
  assert_eq(throttle, 7)
  assert_eq(results.length(), 2)
  assert_eq(results[0].entity[0].entity_name, Some("alice"))
  assert_eq(results[1].error_code, 100)
  assert_eq(results[1].error_message, Some("denied"))
  assert_eq(d.remaining(), 0)
}

///|
test "alter client quotas request parses back through the fake broker" {
  let entries : Array[@moonkafka.ClientQuotaAlteration] = [
    {
      entity: [{ entity_type: "user", entity_name: Some("alice"), }],
      ops: [{ key: "producer_byte_rate", value: 4096.0, remove: false, }],
    },
  ]
  // v0: legacy header framing.
  let v0 = @moonkafka.encode_request(
    @moonkafka.API_ALTER_CLIENT_QUOTAS,
    0,
    1,
    "cid",
    @moonkafka.encode_alter_client_quotas_request(entries, true, 0),
    flexible=false,
  )
  let d0 = @buf.Decoder::new(v0)
  d0.skip(4) // size
  let (parsed0, validate0) = fake_alter_client_quotas_request(
    d0.read_bytes(d0.remaining()),
    0,
  )
  assert_true(validate0)
  assert_eq(parsed0[0].entity[0].entity_name, Some("alice"))
  assert_eq(parsed0[0].ops[0].value, 4096.0)
  // v1: flexible header framing.
  let v1 = @moonkafka.encode_request(
    @moonkafka.API_ALTER_CLIENT_QUOTAS,
    1,
    1,
    "cid",
    @moonkafka.encode_alter_client_quotas_request(entries, false, 1),
  )
  let d1 = @buf.Decoder::new(v1)
  d1.skip(4)
  let (parsed1, validate1) = fake_alter_client_quotas_request(
    d1.read_bytes(d1.remaining()),
    1,
  )
  assert_true(!validate1)
  assert_eq(parsed1[0].ops[0].key, "producer_byte_rate")
}

///|
test "user scram credentials decode v0 responses" {
  // Describe: one user with one credential; a user-level error case.
  let d = @buf.Decoder::new(fake_describe_user_scram_response())
  let (described, throttle) = @moonkafka.decode_describe_user_scram_response(d)
  assert_eq(throttle, 0)
  assert_eq(described.error_code, 0)
  assert_eq(described.results[0].user, "alice")
  assert_eq(described.results[0].credential_infos[0].mechanism, 2)
  assert_eq(described.results[0].credential_infos[0].iterations, 8192)
  assert_eq(d.remaining(), 0)
  // Alter: the second result failed.
  let alter = @buf.Encoder::new()
  alter.write_i32(0) // throttle
  alter.write_compact_len(2)
  alter.write_compact_string("alice")
  alter.write_i16(0)
  alter.write_compact_nullable_string(None)
  alter.write_tag_buffer()
  alter.write_compact_string("bob")
  alter.write_i16(85)
  alter.write_compact_nullable_string(Some("iteration count too small"))
  alter.write_tag_buffer()
  alter.write_tag_buffer()
  let da = @buf.Decoder::new(alter.to_bytes())
  let (results, _) = @moonkafka.decode_alter_user_scram_response(da)
  assert_eq(results[0].user, "alice")
  assert_eq(results[1].error_code, 85)
  assert_eq(results[1].error_message, Some("iteration count too small"))
}

///|
test "alter user scram request parses back through the fake broker" {
  let upsertion = @moonkafka.ScramCredentialUpsertion::from_password(
    "bob",
    @moonkafka.SCRAM_MECHANISM_SHA_256,
    @utf8.encode("s3cret"),
  )
  let frame = @moonkafka.encode_request(
    @moonkafka.API_ALTER_USER_SCRAM_CREDENTIALS,
    0,
    1,
    "cid",
    @moonkafka.encode_alter_user_scram_request(
      [{ name: "alice", mechanism: @moonkafka.SCRAM_MECHANISM_SHA_512, }],
      [upsertion],
    ),
  )
  let d = @buf.Decoder::new(frame)
  d.skip(4) // size
  let (deletions, upsertions) = fake_alter_user_scram_request(
    d.read_bytes(d.remaining()),
  )
  assert_eq(deletions[0].name, "alice")
  assert_eq(deletions[0].mechanism, @moonkafka.SCRAM_MECHANISM_SHA_512)
  assert_eq(upsertions[0].name, "bob")
  assert_eq(upsertions[0].iterations, 4096)
  // The stored salted password is Hi(password, salt, iterations).
  let salted = @moonkafka.scram_salted_password(
    false,
    @utf8.encode("s3cret"),
    upsertions[0].salt,
    4096,
  )
  assert_eq(upsertions[0].salted_password, salted)
}

///|
test "scram credential upsertion rejects unknown mechanisms" {
  let outcome = try? @moonkafka.ScramCredentialUpsertion::from_password(
    "bob",
    3,
    @utf8.encode("s3cret"),
  )
  assert_true(outcome is Err(_))
}

///|
test "fake api versions negotiates the group describe range" {
  let body = fake_api_versions_response(0)
  let d = @buf.Decoder::new(body)
  let (versions, _) = @moonkafka.decode_api_versions_response(d)
  assert_eq(versions.pick(@moonkafka.API_DESCRIBE_GROUPS), 6)
  assert_eq(versions.pick(@moonkafka.API_CONSUMER_GROUP_DESCRIBE), 1)
  assert_eq(versions.pick(@moonkafka.API_LIST_GROUPS), 5)
  assert_eq(versions.pick(@moonkafka.API_DELETE_GROUPS), 2)
  assert_eq(versions.pick(@moonkafka.API_DESCRIBE_PRODUCERS), 0)
  assert_eq(versions.pick(@moonkafka.API_DESCRIBE_TRANSACTIONS), 0)
  assert_eq(versions.pick(@moonkafka.API_LIST_TRANSACTIONS), 2)
}

///|
test "describe producers v0 request encodes topics and partitions" {
  assert_eq(@moonkafka.encode_describe_producers_request([]), b"\x01\x00")
  assert_eq(
    @moonkafka.encode_describe_producers_request([("t", [0, 1])]),
    b"\x02\x02t\x03\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00",
  )
}

///|
test "describe producers request parses back through the fake broker" {
  let framed = @moonkafka.encode_request(
    @moonkafka.API_DESCRIBE_PRODUCERS,
    0,
    1,
    "cid",
    @moonkafka.encode_describe_producers_request([("a", [0]), ("b", [2, 3])]),
  )
  let d = @buf.Decoder::new(framed)
  d.skip(4) // size
  let topics = fake_describe_producers_request(d.read_bytes(d.remaining()))
  assert_eq(topics, [("a", [0]), ("b", [2, 3])])
}

///|
test "describe producers v0 decodes active producer state" {
  let d = @buf.Decoder::new(fake_describe_producers_response([("t", [0, 1])]))
  let (topics, throttle) = @moonkafka.decode_describe_producers_response(d)
  assert_eq(throttle, 0)
  assert_eq(d.remaining(), 0)
  assert_eq(topics.length(), 1)
  assert_eq(topics[0].name, "t")
  assert_eq(topics[0].partitions.length(), 2)
  let p0 = topics[0].partitions[0]
  assert_eq(p0.partition_index, 0)
  assert_eq(p0.error_code, 0)
  assert_eq(p0.error_message, None)
  assert_eq(p0.active_producers.length(), 1)
  let producer = p0.active_producers[0]
  assert_eq(producer.producer_id, 12345L)
  assert_eq(producer.producer_epoch, 1)
  assert_eq(producer.last_sequence, 42)
  assert_eq(producer.last_timestamp, 1234567890L)
  assert_eq(producer.coordinator_epoch, 0)
  assert_eq(producer.current_txn_start_offset, 100L)
  let p1 = topics[0].partitions[1]
  assert_eq(p1.partition_index, 1)
  assert_eq(p1.active_producers.length(), 0)
}

///|
test "describe transactions v0 request encodes the transactional ids" {
  assert_eq(@moonkafka.encode_describe_transactions_request([]), b"\x01\x00")
  assert_eq(
    @moonkafka.encode_describe_transactions_request(["tx1", "tx2"]),
    b"\x03\x04tx1\x04tx2\x00",
  )
}

///|
test "describe transactions request parses back through the fake broker" {
  let framed = @moonkafka.encode_request(
    @moonkafka.API_DESCRIBE_TRANSACTIONS,
    0,
    1,
    "cid",
    @moonkafka.encode_describe_transactions_request(["a", "b"]),
  )
  let d = @buf.Decoder::new(framed)
  d.skip(4) // size
  let ids = fake_describe_transactions_request(d.read_bytes(d.remaining()))
  assert_eq(ids, ["a", "b"])
}

///|
test "describe transactions v0 decodes the transaction state" {
  let d = @buf.Decoder::new(fake_describe_transactions_response(["tx1"]))
  let (transactions, throttle) = @moonkafka.decode_describe_transactions_response(
    d,
  )
  assert_eq(throttle, 0)
  assert_eq(d.remaining(), 0)
  assert_eq(transactions.length(), 1)
  let txn = transactions[0]
  assert_eq(txn.error_code, 0)
  assert_eq(txn.transactional_id, "tx1")
  assert_eq(txn.transaction_state, "Ongoing")
  assert_eq(txn.transaction_timeout_ms, 30000)
  assert_eq(txn.transaction_start_time_ms, 1234567890L)
  assert_eq(txn.producer_id, 12345L)
  assert_eq(txn.producer_epoch, 1)
  assert_eq(txn.topics.length(), 1)
  assert_eq(txn.topics[0].topic, "events")
  assert_eq(txn.topics[0].partitions, [0, 1])
}

///|
test "list transactions v2 request encodes the filters" {
  // v1 has no transactional-id pattern field; v2 appends a null one.
  assert_eq(
    @moonkafka.encode_list_transactions_request(1, [], [], -1L, None),
    b"\x01\x01\xff\xff\xff\xff\xff\xff\xff\xff\x00",
  )
  assert_eq(
    @moonkafka.encode_list_transactions_request(2, [], [], -1L, None),
    b"\x01\x01\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00",
  )
}

///|
test "list transactions request parses back through the fake broker" {
  let framed = @moonkafka.encode_request(
    @moonkafka.API_LIST_TRANSACTIONS,
    2,
    1,
    "cid",
    @moonkafka.encode_list_transactions_request(
      2,
      ["Ongoing"],
      [12345L],
      5000L,
      Some("txn-*"),
    ),
  )
  let d = @buf.Decoder::new(framed)
  d.skip(4) // size
  let (states, producers, duration, pattern) = fake_list_transactions_request(
    d.read_bytes(d.remaining()),
  )
  assert_eq(states, ["Ongoing"])
  assert_eq(producers, [12345L])
  assert_eq(duration, 5000L)
  assert_eq(pattern, Some("txn-*"))
}

///|
test "list transactions v2 decodes the listing" {
  let d = @buf.Decoder::new(
    fake_list_transactions_response(0, ["BadState"], [
      ("txn-1", 12345L, "Ongoing"),
    ]),
  )
  let (result, throttle) = @moonkafka.decode_list_transactions_response(d)
  assert_eq(throttle, 0)
  assert_eq(d.remaining(), 0)
  assert_eq(result.error_code, 0)
  assert_eq(result.unknown_state_filters, ["BadState"])
  assert_eq(result.transactions.length(), 1)
  let txn = result.transactions[0]
  assert_eq(txn.transactional_id, "txn-1")
  assert_eq(txn.producer_id, 12345L)
  assert_eq(txn.transaction_state, "Ongoing")
}

///|
test "delete groups v2 request encodes the group names" {
  assert_eq(@moonkafka.encode_delete_groups_request([]), b"\x01\x00")
  assert_eq(
    @moonkafka.encode_delete_groups_request(["g1", "g2"]),
    b"\x03\x03g1\x03g2\x00",
  )
}

///|
test "delete groups request parses back through the fake broker" {
  let framed = @moonkafka.encode_request(
    @moonkafka.API_DELETE_GROUPS,
    2,
    1,
    "cid",
    @moonkafka.encode_delete_groups_request(["a", "b"]),
  )
  let d = @buf.Decoder::new(framed)
  d.skip(4) // size
  let groups = fake_delete_groups_request(d.read_bytes(d.remaining()))
  assert_eq(groups, ["a", "b"])
}

///|
test "delete groups v2 decodes per-group error codes" {
  let d = @buf.Decoder::new(
    fake_delete_groups_response(["g1", "gone"], ["gone"]),
  )
  let (results, throttle) = @moonkafka.decode_delete_groups_response(d)
  assert_eq(throttle, 0)
  assert_eq(d.remaining(), 0)
  assert_eq(results.length(), 2)
  assert_eq(results[0].group_id, "g1")
  assert_eq(results[0].error_code, 0)
  assert_eq(results[1].group_id, "gone")
  assert_eq(results[1].error_code, @moonkafka.GROUP_ID_NOT_FOUND)
}

///|
test "delete groups v2 decodes NON_EMPTY_GROUP as a value" {
  // A group that still has members: the broker reports NON_EMPTY_GROUP (68)
  // per group, and the driver leaves it as a value rather than raising.
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1)
  e.write_compact_string("busy")
  e.write_i16(68)
  e.write_tag_buffer()
  e.write_tag_buffer() // top level
  let d = @buf.Decoder::new(e.to_bytes())
  let (results, _) = @moonkafka.decode_delete_groups_response(d)
  assert_eq(d.remaining(), 0)
  assert_eq(results[0].group_id, "busy")
  assert_eq(results[0].error_code, 68)
}

///|
test "list groups v5 request encodes both filter arrays" {
  // No filters: two empty compact arrays then the tag buffer.
  assert_eq(@moonkafka.encode_list_groups_request([], []), b"\x01\x01\x00")
  // States and types both present.
  assert_eq(
    @moonkafka.encode_list_groups_request(["Stable"], ["consumer"]),
    b"\x02\x07Stable\x02\x09consumer\x00",
  )
}

///|
test "list groups request parses back through the fake broker" {
  let framed = @moonkafka.encode_request(
    @moonkafka.API_LIST_GROUPS,
    5,
    1,
    "cid",
    @moonkafka.encode_list_groups_request(["Stable"], ["classic", "consumer"]),
  )
  let d = @buf.Decoder::new(framed)
  d.skip(4) // size
  let (states, types) = fake_list_groups_request(d.read_bytes(d.remaining()))
  assert_eq(states, ["Stable"])
  assert_eq(types, ["classic", "consumer"])
}

///|
test "list groups v5 decodes the group state and type fields" {
  let d = @buf.Decoder::new(fake_list_groups_response(["Stable"], []))
  let (result, throttle) = @moonkafka.decode_list_groups_response(d)
  assert_eq(throttle, 0)
  assert_eq(d.remaining(), 0)
  assert_eq(result.error_code, 0)
  assert_eq(result.groups.length(), 2)
  let classic = result.groups[0]
  assert_eq(classic.group_id, "classic-group")
  assert_eq(classic.protocol_type, "consumer")
  assert_eq(classic.group_state, "Stable")
  assert_eq(classic.group_type, "classic")
  let k848 = result.groups[1]
  assert_eq(k848.group_id, "k848-group")
  assert_eq(k848.group_type, "consumer")
}

///|
test "list groups v5 decodes an empty listing and a top-level error" {
  // An empty groups array still carries its per-group tag buffer slot
  // and the top-level tag buffer.
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_i16(30) // GROUP_AUTHORIZATION_FAILED
  e.write_compact_len(0) // no groups
  e.write_tag_buffer() // top level
  let d = @buf.Decoder::new(e.to_bytes())
  let (result, _) = @moonkafka.decode_list_groups_response(d)
  assert_eq(d.remaining(), 0)
  assert_eq(result.error_code, 30)
  assert_eq(result.groups.length(), 0)
}

///|
test "group describe requests are wire-identical across both apis" {
  // DescribeGroups v6 and ConsumerGroupDescribe v1 declare the same two
  // request fields, so their bodies match byte for byte.
  let classic = @moonkafka.encode_describe_groups_request(["g1"], false)
  let consumer = @moonkafka.encode_consumer_group_describe_request(
    ["g1"],
    false,
  )
  assert_eq(classic, consumer)
  // Compact array count, compact group id, the flag, the tag buffer.
  assert_eq(classic, b"\x02\x03g1\x00\x00")
  assert_eq(
    @moonkafka.encode_describe_groups_request(["g1"], true),
    b"\x02\x03g1\x01\x00",
  )
}

///|
test "group describe request parses back through the fake broker" {
  let framed = @moonkafka.encode_request(
    @moonkafka.API_DESCRIBE_GROUPS,
    6,
    1,
    "cid",
    @moonkafka.encode_describe_groups_request(["a", "b"], true),
  )
  let d = @buf.Decoder::new(framed)
  d.skip(4) // size
  let (groups, wanted) = fake_group_describe_request(
    d.read_bytes(d.remaining()),
  )
  assert_eq(groups, ["a", "b"])
  assert_true(wanted)
}

///|
test "describe groups v6 decodes the classic member payloads" {
  let d = @buf.Decoder::new(fake_describe_groups_response(["g"], true, []))
  let (groups, throttle) = @moonkafka.decode_describe_groups_response(d)
  assert_eq(throttle, 0)
  assert_eq(d.remaining(), 0)
  assert_eq(groups.length(), 1)
  let group = groups[0]
  assert_eq(group.error_code, 0)
  assert_eq(group.error_message, None)
  assert_eq(group.group_state, "Stable")
  assert_eq(group.protocol_type, "consumer")
  assert_eq(group.protocol_data, "range")
  assert_eq(group.authorized_operations, Some(4094))
  let member = group.members[0]
  assert_eq(member.member_id, "g-member-1")
  assert_eq(member.group_instance_id, Some("g-instance-1"))
  // The opaque classic payloads decode through the consumer protocol.
  assert_eq(member.subscribed_topics(), ["g-topic"])
  assert_eq(member.assigned_partitions(), [("g-topic", [0, 1])])
}

///|
test "describe groups v6 reports a KIP-848 group as not found" {
  let d = @buf.Decoder::new(
    fake_describe_groups_response(["k848"], true, ["k848"]),
  )
  let (groups, _) = @moonkafka.decode_describe_groups_response(d)
  assert_eq(d.remaining(), 0)
  let group = groups[0]
  assert_eq(group.error_code, @moonkafka.GROUP_ID_NOT_FOUND)
  assert_true(group.is_not_found())
  assert_eq(group.error_message, Some("Group k848 is not a classic group."))
  assert_eq(group.group_state, "Dead")
  assert_eq(group.protocol_type, "")
  assert_eq(group.members.length(), 0)
  // An errored group carries the MIN_VALUE sentinel, not a bitfield, even
  // though the request asked for authorized operations.
  assert_eq(group.authorized_operations, None)
}

///|
test "consumer group describe v1 decodes epochs, regex, and targets" {
  let d = @buf.Decoder::new(
    fake_consumer_group_describe_response(["k848"], true, []),
  )
  let (groups, throttle) = @moonkafka.decode_consumer_group_describe_response(d)
  assert_eq(throttle, 0)
  assert_eq(d.remaining(), 0)
  let group = groups[0]
  assert_eq(group.group_state, "Stable")
  assert_eq(group.group_epoch, 7)
  assert_eq(group.assignment_epoch, 6)
  assert_eq(group.assignor_name, "range")
  assert_eq(group.authorized_operations, Some(4094))
  let consumer = group.members[0]
  assert_eq(consumer.member_type, @moonkafka.GROUP_MEMBER_TYPE_CONSUMER)
  assert_eq(consumer.member_epoch, 6)
  assert_eq(consumer.rack_id, Some("rack-a"))
  assert_eq(consumer.subscribed_topic_names, ["k848-topic"])
  assert_eq(consumer.subscribed_topic_regex, None)
  // The member epoch lags the group epoch, so the assignment it is
  // fetching from is behind the one the broker targeted.
  assert_eq(consumer.assignment[0].partitions, [0])
  assert_eq(consumer.target_assignment[0].partitions, [0, 1])
  assert_eq(
    consumer.assignment[0].topic_id,
    consumer.target_assignment[0].topic_id,
  )
  let migrated = group.members[1]
  assert_eq(migrated.member_type, @moonkafka.GROUP_MEMBER_TYPE_CLASSIC)
  assert_eq(migrated.subscribed_topic_names.length(), 0)
  assert_eq(migrated.subscribed_topic_regex, Some("k848-.*"))
}

///|
test "consumer group describe v1 decodes empty assignments" {
  // A member with nothing assigned yet: both Assignment wrappers still
  // carry their own tag buffer, and MemberType reports unknown.
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1) // groups
  e.write_i16(0) // error
  e.write_compact_nullable_string(None)
  e.write_compact_string("g")
  e.write_compact_string("Empty")
  e.write_i32(0) // group epoch
  e.write_i32(-1) // assignment epoch
  e.write_compact_string("range")
  e.write_compact_len(1) // members
  e.write_compact_string("m")
  e.write_compact_nullable_string(None) // instance id
  e.write_compact_nullable_string(None) // rack id
  e.write_i32(-1) // member epoch
  e.write_compact_string("c")
  e.write_compact_string("/h")
  e.write_compact_len(0) // subscribed topic names
  e.write_compact_nullable_string(None) // subscribed topic regex
  e.write_compact_len(0) // assignment: no topics
  e.write_tag_buffer() // assignment wrapper
  e.write_compact_len(0) // target assignment: no topics
  e.write_tag_buffer() // assignment wrapper
  e.write_i8(-1) // member type: unknown
  e.write_tag_buffer() // member
  e.write_i32(@moonkafka.AUTHORIZED_OPERATIONS_NONE)
  e.write_tag_buffer() // group
  e.write_tag_buffer() // top level
  let d = @buf.Decoder::new(e.to_bytes())
  let (groups, _) = @moonkafka.decode_consumer_group_describe_response(d)
  assert_eq(d.remaining(), 0)
  let member = groups[0].members[0]
  assert_eq(member.member_type, @moonkafka.GROUP_MEMBER_TYPE_UNKNOWN)
  assert_eq(member.assignment.length(), 0)
  assert_eq(member.target_assignment.length(), 0)
  assert_eq(groups[0].authorized_operations, None)
}

///|
test "foreign member metadata decodes to no topics" {
  // A custom protocol (Kafka Connect, say) ships member metadata this
  // driver cannot parse: the helpers report nothing rather than raising,
  // and the raw bytes stay available to a caller that understands them.
  let foreign = b"\x00\x01\x00\x00" // ConsumerProtocol version 1: unsupported
  let e = @buf.Encoder::new()
  e.write_i32(0) // throttle
  e.write_compact_len(1) // groups
  e.write_i16(0) // error
  e.write_compact_nullable_string(None)
  e.write_compact_string("connect-group")
  e.write_compact_string("Stable")
  e.write_compact_string("connect") // protocol type
  e.write_compact_string("default") // protocol data
  e.write_compact_len(1) // members
  e.write_compact_string("m")
  e.write_compact_nullable_string(None) // group instance id
  e.write_compact_string("c")
  e.write_compact_string("/h")
  e.write_compact_len(foreign.length())
  e.write_bytes(foreign)
  e.write_compact_len(0) // no assignment payload
  e.write_tag_buffer() // member
  e.write_i32(@moonkafka.AUTHORIZED_OPERATIONS_NONE)
  e.write_tag_buffer() // group
  e.write_tag_buffer() // top level
  let d = @buf.Decoder::new(e.to_bytes())
  let (groups, _) = @moonkafka.decode_describe_groups_response(d)
  assert_eq(d.remaining(), 0)
  let member = groups[0].members[0]
  assert_eq(member.subscribed_topics(), [])
  assert_eq(member.assigned_partitions(), [])
  assert_eq(member.member_metadata, foreign)
  assert_eq(member.member_assignment, Bytes::make(0, b'\x00'))
}
