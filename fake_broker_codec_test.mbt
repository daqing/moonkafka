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
