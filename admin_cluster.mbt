// Admin cluster/config operations (Phase 5.3): DescribeCluster v0-v2,
// DescribeConfigs v4, IncrementalAlterConfigs v1, AlterConfigs v2,
// ListConfigResources v0-v1, DescribeLogDirs v2-v5, ElectLeaders v2,
// AlterPartitionReassignments v0-v1, ListPartitionReassignments v0,
// UnregisterBroker v0, DescribeQuorum v2, and UpdateFeatures v2. Field
// order is pinned from the message schemas in the Kafka 4.3 tree under
// clients/src/main/resources/common/message/. All per-item error codes
// travel as values; the Admin retry policy re-issues retriable ones.

///|
/// Config resource types (ConfigResource.Type byte values).
pub const CONFIG_RESOURCE_UNKNOWN : Int = 0

///|
pub const CONFIG_RESOURCE_TOPIC : Int = 2

///|
pub const CONFIG_RESOURCE_BROKER : Int = 4

///|
pub const CONFIG_RESOURCE_BROKER_LOGGER : Int = 8

///|
pub const CONFIG_RESOURCE_CLIENT_METRICS : Int = 16

///|
pub const CONFIG_RESOURCE_GROUP : Int = 32

///|
/// Incremental config operations (AlterConfigOp.OpType byte values).
pub const CONFIG_OP_SET : Int = 0

///|
pub const CONFIG_OP_DELETE : Int = 1

///|
pub const CONFIG_OP_APPEND : Int = 2

///|
pub const CONFIG_OP_SUBTRACT : Int = 3

///|
/// Leader election types (ElectionType byte values).
pub const ELECTION_PREFERRED : Int = 0

///|
pub const ELECTION_UNCLEAN : Int = 1

///|
/// DescribeCluster endpoint types (schema: 1 = brokers, 2 = controllers).
pub const ENDPOINT_TYPE_BROKERS : Int = 1

///|
pub const ENDPOINT_TYPE_CONTROLLERS : Int = 2

///|
/// UpdateFeatures upgrade types (UpdateFeaturesRequest.UpgradeType).
pub const FEATURE_UPGRADE : Int = 1

///|
pub const FEATURE_SAFE_DOWNGRADE : Int = 2

///|
pub const FEATURE_UNSAFE_DOWNGRADE : Int = 3

///|
/// One cluster broker of a DescribeCluster result.
pub(all) struct AdminClusterBroker {
  node_id : Int
  host : String
  port : Int
  rack : String?
  /// v2+ only; false below v2.
  is_fenced : Bool
} derive(@debug.Debug)

///|
pub struct AdminClusterDescription {
  error_code : Int
  error_message : String?
  /// The endpoint type the broker answered for (v1+; 1 at v0).
  endpoint_type : Int
  cluster_id : String?
  controller_id : Int
  brokers : Array[AdminClusterBroker]
  /// 32-bit bitfield of cluster operations the caller is authorized
  /// for, when requested; -2147483648 when not.
  cluster_authorized_operations : Int
} derive(@debug.Debug)

///|
/// Encode a DescribeCluster v0-v2 request body.
pub fn encode_describe_cluster_request(
  include_cluster_authorized_operations : Bool,
  include_fenced_brokers : Bool,
  version : Int,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_bool(include_cluster_authorized_operations)
  if version >= 1 {
    body.write_i8(ENDPOINT_TYPE_BROKERS)
  }
  if version >= 2 {
    body.write_bool(include_fenced_brokers)
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DescribeCluster v0-v2 response body.
pub fn decode_describe_cluster_response(
  version : Int,
  d : @buf.Decoder,
) -> (AdminClusterDescription, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  let endpoint_type = if version >= 1 {
    d.read_i8()
  } else {
    ENDPOINT_TYPE_BROKERS
  }
  let cluster_id = d.read_compact_string()
  let controller_id = d.read_i32()
  let n = d.read_compact_len()
  let brokers : Array[AdminClusterBroker] = []
  for _ in 0..<n {
    let node_id = d.read_i32()
    let host = d.read_compact_string()
    let port = d.read_i32()
    let rack = d.read_compact_nullable_string()
    let is_fenced = if version >= 2 { d.read_bool() } else { false }
    d.skip_tag_buffer()
    brokers.push({ node_id, host, port, rack, is_fenced, })
  }
  let authorized = d.read_i32()
  d.skip_tag_buffer()
  (
    {
      error_code,
      error_message,
      endpoint_type,
      cluster_id: Some(cluster_id),
      controller_id,
      brokers,
      cluster_authorized_operations: authorized,
    },
    throttle,
  )
}

///|
pub async fn BrokerConnection::describe_cluster(
  self : BrokerConnection,
  include_authorized_operations? : Bool = false,
  include_fenced_brokers? : Bool = false,
  timeout_ms? : Int = 30000,
) -> AdminClusterDescription {
  let version = self.api_version(API_DESCRIBE_CLUSTER)
  let d = self.request(
    API_DESCRIBE_CLUSTER,
    version,
    encode_describe_cluster_request(
      include_authorized_operations, include_fenced_brokers, version,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_describe_cluster_response(version, d)
  self.note_throttle(throttle)
  result
}

///|
/// One config resource to describe: type + name, with an optional key
/// filter (None lists every key).
pub(all) struct ConfigResourceKey {
  resource_type : Int
  resource_name : String
  config_keys : Array[String]?
} derive(@debug.Debug)

///|
/// One described config entry with its source and synonyms.
pub(all) struct AdminConfigEntry {
  name : String
  value : String?
  read_only : Bool
  config_source : Int
  is_sensitive : Bool
  /// (name, value, source) triples.
  synonyms : Array[(String, String?, Int)]
  config_type : Int
  documentation : String?
} derive(@debug.Debug)

///|
pub struct AdminConfigsResult {
  error_code : Int
  error_message : String?
  resource_type : Int
  resource_name : String
  configs : Array[AdminConfigEntry]
} derive(@debug.Debug)

///|
/// Encode a DescribeConfigs v4 request body.
pub fn encode_describe_configs_request(
  resources : Array[ConfigResourceKey],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(resources.length())
  for resource in resources {
    body.write_i8(resource.resource_type)
    body.write_compact_string(resource.resource_name)
    match resource.config_keys {
      Some(keys) => {
        body.write_compact_len(keys.length())
        for key in keys {
          body.write_compact_string(key)
        }
      }
      None => body.write_compact_len(-1)
    }
    body.write_tag_buffer()
  }
  body.write_bool(false) // include_synonyms
  body.write_bool(false) // include_documentation
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DescribeConfigs v4 response body.
pub fn decode_describe_configs_response(
  d : @buf.Decoder,
) -> (Array[AdminConfigsResult], Int) raise {
  let throttle = d.read_i32()
  let out : Array[AdminConfigsResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let error_code = d.read_i16()
    let error_message = d.read_compact_nullable_string()
    let resource_type = d.read_i8()
    let resource_name = d.read_compact_string()
    let configs : Array[AdminConfigEntry] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      let name = d.read_compact_string()
      let value = d.read_compact_nullable_string()
      let read_only = d.read_bool()
      let config_source = d.read_i8()
      let is_sensitive = d.read_bool()
      let synonyms : Array[(String, String?, Int)] = []
      let synonym_count = d.read_compact_len()
      for _ in 0..<synonym_count {
        let synonym = d.read_compact_string()
        let synonym_value = d.read_compact_nullable_string()
        let source = d.read_i8()
        d.skip_tag_buffer()
        synonyms.push((synonym, synonym_value, source))
      }
      let config_type = d.read_i8()
      let documentation = d.read_compact_nullable_string()
      d.skip_tag_buffer()
      configs.push({
        name,
        value,
        read_only,
        config_source,
        is_sensitive,
        synonyms,
        config_type,
        documentation,
      })
    }
    d.skip_tag_buffer()
    out.push({
      error_code,
      error_message,
      resource_type,
      resource_name,
      configs,
    })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::describe_configs(
  self : BrokerConnection,
  resources : Array[ConfigResourceKey],
  timeout_ms? : Int = 30000,
) -> Array[AdminConfigsResult] {
  let d = self.request(
    API_DESCRIBE_CONFIGS,
    self.api_version(API_DESCRIBE_CONFIGS),
    encode_describe_configs_request(resources),
    timeout_ms~,
  )
  let (results, throttle) = decode_describe_configs_response(d)
  self.note_throttle(throttle)
  results
}

///|
/// One incremental config update: an operation applied to one key.
pub(all) struct AlterableConfig {
  name : String
  operation : Int
  /// None for CONFIG_OP_DELETE.
  value : String?
} derive(@debug.Debug)

///|
/// One resource's incremental updates.
pub(all) struct AlterableConfigResource {
  resource_type : Int
  resource_name : String
  configs : Array[AlterableConfig]
} derive(@debug.Debug)

///|
pub struct AlterConfigsResult {
  error_code : Int
  error_message : String?
  resource_type : Int
  resource_name : String
} derive(@debug.Debug)

///|
/// Encode an IncrementalAlterConfigs v1 request body.
pub fn encode_incremental_alter_configs_request(
  resources : Array[AlterableConfigResource],
  validate_only : Bool,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(resources.length())
  for resource in resources {
    body.write_i8(resource.resource_type)
    body.write_compact_string(resource.resource_name)
    body.write_compact_len(resource.configs.length())
    for config in resource.configs {
      body.write_compact_string(config.name)
      body.write_i8(config.operation)
      body.write_compact_nullable_string(config.value)
      body.write_tag_buffer()
    }
    body.write_tag_buffer()
  }
  body.write_bool(validate_only)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an IncrementalAlterConfigs v1 response body.
pub fn decode_incremental_alter_configs_response(
  d : @buf.Decoder,
) -> (Array[AlterConfigsResult], Int) raise {
  let throttle = d.read_i32()
  let out : Array[AlterConfigsResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let error_code = d.read_i16()
    let error_message = d.read_compact_nullable_string()
    let resource_type = d.read_i8()
    let resource_name = d.read_compact_string()
    d.skip_tag_buffer()
    out.push({ error_code, error_message, resource_type, resource_name, })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::incremental_alter_configs(
  self : BrokerConnection,
  resources : Array[AlterableConfigResource],
  timeout_ms? : Int = 30000,
  validate_only? : Bool = false,
) -> Array[AlterConfigsResult] {
  let d = self.request(
    API_INCREMENTAL_ALTER_CONFIGS,
    self.api_version(API_INCREMENTAL_ALTER_CONFIGS),
    encode_incremental_alter_configs_request(resources, validate_only),
    timeout_ms~,
  )
  let (results, throttle) = decode_incremental_alter_configs_response(d)
  self.note_throttle(throttle)
  results
}

///|
/// One legacy (full-replace) config override.
pub(all) struct SettableConfig {
  name : String
  value : String?
} derive(@debug.Debug)

///|
/// One resource's full config replacement (AlterConfigs semantics: the
/// given list replaces the resource's overridable configs).
pub(all) struct SettableConfigResource {
  resource_type : Int
  resource_name : String
  configs : Array[SettableConfig]
} derive(@debug.Debug)

///|
/// Encode an AlterConfigs v2 request body.
pub fn encode_alter_configs_request(
  resources : Array[SettableConfigResource],
  validate_only : Bool,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(resources.length())
  for resource in resources {
    body.write_i8(resource.resource_type)
    body.write_compact_string(resource.resource_name)
    body.write_compact_len(resource.configs.length())
    for config in resource.configs {
      body.write_compact_string(config.name)
      body.write_compact_nullable_string(config.value)
      body.write_tag_buffer()
    }
    body.write_tag_buffer()
  }
  body.write_bool(validate_only)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an AlterConfigs v2 response body (same shape as
/// IncrementalAlterConfigs).
pub fn decode_alter_configs_response(
  d : @buf.Decoder,
) -> (Array[AlterConfigsResult], Int) raise {
  decode_incremental_alter_configs_response(d)
}

///|
pub async fn BrokerConnection::alter_configs(
  self : BrokerConnection,
  resources : Array[SettableConfigResource],
  timeout_ms? : Int = 30000,
  validate_only? : Bool = false,
) -> Array[AlterConfigsResult] {
  let d = self.request(
    API_ALTER_CONFIGS,
    self.api_version(API_ALTER_CONFIGS),
    encode_alter_configs_request(resources, validate_only),
    timeout_ms~,
  )
  let (results, throttle) = decode_alter_configs_response(d)
  self.note_throttle(throttle)
  results
}

///|
/// One config resource the broker lists: name plus type (v1+; 0 below).
pub(all) struct ConfigResourceListing {
  resource_name : String
  resource_type : Int
} derive(@debug.Debug)

///|
pub struct ListConfigResourcesResult {
  error_code : Int
  resources : Array[ConfigResourceListing]
} derive(@debug.Debug)

///|
/// Encode a ListConfigResources v0/v1 request body: v1 filters by
/// resource type (an empty filter means all supported types).
pub fn encode_list_config_resources_request(
  resource_types : Array[Int],
  version : Int,
) -> Bytes {
  let body = @buf.Encoder::new()
  if version >= 1 {
    body.write_compact_len(resource_types.length())
    for resource_type in resource_types {
      body.write_i8(resource_type)
    }
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a ListConfigResources v0/v1 response body.
pub fn decode_list_config_resources_response(
  version : Int,
  d : @buf.Decoder,
) -> (ListConfigResourcesResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let resources : Array[ConfigResourceListing] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let resource_name = d.read_compact_string()
    let resource_type = if version >= 1 { d.read_i8() } else { 0 }
    d.skip_tag_buffer()
    resources.push({ resource_name, resource_type, })
  }
  d.skip_tag_buffer()
  ({ error_code, resources, }, throttle)
}

///|
pub async fn BrokerConnection::list_config_resources(
  self : BrokerConnection,
  resource_types? : Array[Int] = [],
  timeout_ms? : Int = 30000,
) -> ListConfigResourcesResult {
  let version = self.api_version(API_LIST_CONFIG_RESOURCES)
  let d = self.request(
    API_LIST_CONFIG_RESOURCES,
    version,
    encode_list_config_resources_request(resource_types, version),
    timeout_ms~,
  )
  let (result, throttle) = decode_list_config_resources_response(version, d)
  self.note_throttle(throttle)
  result
}

///|
/// One partition of a log-dir result.
pub(all) struct LogDirPartition {
  partition_index : Int
  partition_size : Int64
  offset_lag : Int64
  is_future : Bool
} derive(@debug.Debug)

///|
pub(all) struct LogDirTopic {
  name : String
  partitions : Array[LogDirPartition]
} derive(@debug.Debug)

///|
pub struct LogDirResult {
  error_code : Int
  log_dir : String
  topics : Array[LogDirTopic]
  /// v4+; -1 when the broker did not report volume sizes.
  total_bytes : Int64
  /// v4+; -1 likewise.
  usable_bytes : Int64
  /// v5+ (KIP-1066); false below v5.
  is_cordoned : Bool
} derive(@debug.Debug)

///|
pub struct AdminLogDirs {
  /// Top-level error (v3+; 0 at v2).
  error_code : Int
  results : Array[LogDirResult]
} derive(@debug.Debug)

///|
/// Encode a DescribeLogDirs v2-v5 request body: `topics` None asks for
/// all topics (null array on the wire).
pub fn encode_describe_log_dirs_request(
  topics : Array[(String, Array[Int])]?,
  version : Int,
) -> Bytes {
  ignore(version) // the request shape is identical across v2-v5
  let body = @buf.Encoder::new()
  match topics {
    None => body.write_compact_len(-1)
    Some(topics) => {
      body.write_compact_len(topics.length())
      for entry in topics {
        let (name, partitions) = entry
        body.write_compact_string(name)
        body.write_compact_len(partitions.length())
        for partition in partitions {
          body.write_i32(partition)
        }
        body.write_tag_buffer()
      }
    }
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DescribeLogDirs v2-v5 response body.
pub fn decode_describe_log_dirs_response(
  version : Int,
  d : @buf.Decoder,
) -> (AdminLogDirs, Int) raise {
  let throttle = d.read_i32()
  let top_error = if version >= 3 { d.read_i16() } else { 0 }
  let results : Array[LogDirResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let error_code = d.read_i16()
    let log_dir = d.read_compact_string()
    let topics : Array[LogDirTopic] = []
    let topic_count = d.read_compact_len()
    for _ in 0..<topic_count {
      let name = d.read_compact_string()
      let partitions : Array[LogDirPartition] = []
      let partition_count = d.read_compact_len()
      for _ in 0..<partition_count {
        let partition_index = d.read_i32()
        let partition_size = d.read_i64()
        let offset_lag = d.read_i64()
        let is_future = d.read_bool()
        d.skip_tag_buffer()
        partitions.push({
          partition_index,
          partition_size,
          offset_lag,
          is_future,
        })
      }
      d.skip_tag_buffer()
      topics.push({ name, partitions, })
    }
    let total_bytes : Int64 = if version >= 4 { d.read_i64() } else { -1L }
    let usable_bytes : Int64 = if version >= 4 { d.read_i64() } else { -1L }
    let is_cordoned = if version >= 5 { d.read_bool() } else { false }
    d.skip_tag_buffer()
    results.push({
      error_code,
      log_dir,
      topics,
      total_bytes,
      usable_bytes,
      is_cordoned,
    })
  }
  d.skip_tag_buffer()
  ({ error_code: top_error, results, }, throttle)
}

///|
pub async fn BrokerConnection::describe_log_dirs(
  self : BrokerConnection,
  topics? : Array[(String, Array[Int])]? = None,
  timeout_ms? : Int = 30000,
) -> AdminLogDirs {
  let version = self.api_version(API_DESCRIBE_LOG_DIRS)
  let d = self.request(
    API_DESCRIBE_LOG_DIRS,
    version,
    encode_describe_log_dirs_request(topics, version),
    timeout_ms~,
  )
  let (result, throttle) = decode_describe_log_dirs_response(version, d)
  self.note_throttle(throttle)
  result
}

///|
pub struct ElectLeadersPartitionResult {
  partition : Int
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
pub struct ElectLeadersTopicResult {
  topic : String
  partitions : Array[ElectLeadersPartitionResult]
} derive(@debug.Debug)

///|
pub struct AdminElectionResult {
  /// Top-level error code (v1+).
  error_code : Int
  results : Array[ElectLeadersTopicResult]
} derive(@debug.Debug)

///|
/// Encode an ElectLeaders v2 request body: `topic_partitions` None
/// elects for every partition (null array on the wire).
pub fn encode_elect_leaders_request(
  election_type : Int,
  topic_partitions : Array[(String, Array[Int])]?,
  timeout_ms : Int,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_i8(election_type)
  match topic_partitions {
    None => body.write_compact_len(-1)
    Some(topics) => {
      body.write_compact_len(topics.length())
      for entry in topics {
        let (name, partitions) = entry
        body.write_compact_string(name)
        body.write_compact_len(partitions.length())
        for partition in partitions {
          body.write_i32(partition)
        }
        body.write_tag_buffer()
      }
    }
  }
  body.write_i32(timeout_ms)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an ElectLeaders v2 response body.
pub fn decode_elect_leaders_response(
  d : @buf.Decoder,
) -> (AdminElectionResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let results : Array[ElectLeadersTopicResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let topic = d.read_compact_string()
    let partitions : Array[ElectLeadersPartitionResult] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      let partition = d.read_i32()
      let partition_error = d.read_i16()
      let error_message = d.read_compact_nullable_string()
      d.skip_tag_buffer()
      partitions.push({ partition, error_code: partition_error, error_message, })
    }
    d.skip_tag_buffer()
    results.push({ topic, partitions, })
  }
  d.skip_tag_buffer()
  ({ error_code, results, }, throttle)
}

///|
pub async fn BrokerConnection::elect_leaders(
  self : BrokerConnection,
  election_type : Int,
  topic_partitions? : Array[(String, Array[Int])]? = None,
  timeout_ms? : Int = 30000,
) -> AdminElectionResult {
  let d = self.request(
    API_ELECT_LEADERS,
    self.api_version(API_ELECT_LEADERS),
    encode_elect_leaders_request(election_type, topic_partitions, timeout_ms),
    timeout_ms~,
  )
  let (result, throttle) = decode_elect_leaders_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// One partition to reassign: replica broker ids, or None to cancel a
/// pending reassignment.
pub(all) struct ReassignablePartition {
  partition_index : Int
  replicas : Array[Int]?
} derive(@debug.Debug)

///|
pub(all) struct ReassignableTopic {
  name : String
  partitions : Array[ReassignablePartition]
} derive(@debug.Debug)

///|
pub struct ReassignablePartitionResponse {
  partition_index : Int
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
pub struct AdminReassignmentResult {
  error_code : Int
  error_message : String?
  /// (topic, per-partition responses) pairs.
  responses : Array[(String, Array[ReassignablePartitionResponse])]
} derive(@debug.Debug)

///|
/// Encode an AlterPartitionReassignments v0/v1 request body.
pub fn encode_alter_partition_reassignments_request(
  topics : Array[ReassignableTopic],
  timeout_ms : Int,
  allow_replication_factor_change : Bool,
  version : Int,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_i32(timeout_ms)
  if version >= 1 {
    body.write_bool(allow_replication_factor_change)
  }
  body.write_compact_len(topics.length())
  for topic in topics {
    body.write_compact_string(topic.name)
    body.write_compact_len(topic.partitions.length())
    for partition in topic.partitions {
      body.write_i32(partition.partition_index)
      match partition.replicas {
        Some(replicas) => {
          body.write_compact_len(replicas.length())
          for replica in replicas {
            body.write_i32(replica)
          }
        }
        None => body.write_compact_len(-1)
      }
      body.write_tag_buffer()
    }
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an AlterPartitionReassignments v0/v1 response body.
pub fn decode_alter_partition_reassignments_response(
  version : Int,
  d : @buf.Decoder,
) -> (AdminReassignmentResult, Int) raise {
  let throttle = d.read_i32()
  if version >= 1 {
    ignore(d.read_bool()) // allow_replication_factor_change echo
  }
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  let responses : Array[(String, Array[ReassignablePartitionResponse])] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let name = d.read_compact_string()
    let partitions : Array[ReassignablePartitionResponse] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      let partition_index = d.read_i32()
      let partition_error = d.read_i16()
      let partition_message = d.read_compact_nullable_string()
      d.skip_tag_buffer()
      partitions.push({
        partition_index,
        error_code: partition_error,
        error_message: partition_message,
      })
    }
    d.skip_tag_buffer()
    responses.push((name, partitions))
  }
  d.skip_tag_buffer()
  ({ error_code, error_message, responses, }, throttle)
}

///|
pub async fn BrokerConnection::alter_partition_reassignments(
  self : BrokerConnection,
  topics : Array[ReassignableTopic],
  timeout_ms? : Int = 30000,
  allow_replication_factor_change? : Bool = true,
) -> AdminReassignmentResult {
  let version = self.api_version(API_ALTER_PARTITION_REASSIGNMENTS)
  let d = self.request(
    API_ALTER_PARTITION_REASSIGNMENTS,
    version,
    encode_alter_partition_reassignments_request(
      topics, timeout_ms, allow_replication_factor_change, version,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_alter_partition_reassignments_response(
    version, d,
  )
  self.note_throttle(throttle)
  result
}

///|
pub struct OngoingPartitionReassignment {
  partition_index : Int
  replicas : Array[Int]
  adding_replicas : Array[Int]
  removing_replicas : Array[Int]
} derive(@debug.Debug)

///|
pub struct AdminOngoingReassignments {
  error_code : Int
  error_message : String?
  /// (topic, ongoing partitions) pairs.
  topics : Array[(String, Array[OngoingPartitionReassignment])]
} derive(@debug.Debug)

///|
/// Encode a ListPartitionReassignments v0 request body: `topics` None
/// lists every ongoing reassignment.
pub fn encode_list_partition_reassignments_request(
  topics : Array[(String, Array[Int])]?,
  timeout_ms : Int,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_i32(timeout_ms)
  match topics {
    None => body.write_compact_len(-1)
    Some(topics) => {
      body.write_compact_len(topics.length())
      for entry in topics {
        let (name, partitions) = entry
        body.write_compact_string(name)
        body.write_compact_len(partitions.length())
        for partition in partitions {
          body.write_i32(partition)
        }
        body.write_tag_buffer()
      }
    }
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a ListPartitionReassignments v0 response body.
pub fn decode_list_partition_reassignments_response(
  d : @buf.Decoder,
) -> (AdminOngoingReassignments, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  let topics : Array[(String, Array[OngoingPartitionReassignment])] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let name = d.read_compact_string()
    let partitions : Array[OngoingPartitionReassignment] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      let partition_index = d.read_i32()
      let replicas = read_int32_array(d)
      let adding = read_int32_array(d)
      let removing = read_int32_array(d)
      d.skip_tag_buffer()
      partitions.push({
        partition_index,
        replicas,
        adding_replicas: adding,
        removing_replicas: removing,
      })
    }
    d.skip_tag_buffer()
    topics.push((name, partitions))
  }
  d.skip_tag_buffer()
  ({ error_code, error_message, topics, }, throttle)
}

///|
/// Read one compact int32 array.
fn read_int32_array(d : @buf.Decoder) -> Array[Int] raise {
  let n = d.read_compact_len()
  let out : Array[Int] = []
  for _ in 0..<n {
    out.push(d.read_i32())
  }
  out
}

///|
pub async fn BrokerConnection::list_partition_reassignments(
  self : BrokerConnection,
  topics? : Array[(String, Array[Int])]? = None,
  timeout_ms? : Int = 30000,
) -> AdminOngoingReassignments {
  let d = self.request(
    API_LIST_PARTITION_REASSIGNMENTS,
    self.api_version(API_LIST_PARTITION_REASSIGNMENTS),
    encode_list_partition_reassignments_request(topics, timeout_ms),
    timeout_ms~,
  )
  let (result, throttle) = decode_list_partition_reassignments_response(d)
  self.note_throttle(throttle)
  result
}

///|
pub struct UnregisterBrokerResult {
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
/// Encode an UnregisterBroker v0 request body.
pub fn encode_unregister_broker_request(broker_id : Int) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_i32(broker_id)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an UnregisterBroker v0 response body.
pub fn decode_unregister_broker_response(
  d : @buf.Decoder,
) -> (UnregisterBrokerResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  d.skip_tag_buffer()
  ({ error_code, error_message, }, throttle)
}

///|
pub async fn BrokerConnection::unregister_broker(
  self : BrokerConnection,
  broker_id : Int,
  timeout_ms? : Int = 30000,
) -> UnregisterBrokerResult {
  let d = self.request(
    API_UNREGISTER_BROKER,
    self.api_version(API_UNREGISTER_BROKER),
    encode_unregister_broker_request(broker_id),
    timeout_ms~,
  )
  let (result, throttle) = decode_unregister_broker_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// One quorum voter's / observer's progress.
pub(all) struct QuorumReplicaState {
  replica_id : Int
  /// v2+.
  replica_directory_id : Uuid
  log_end_offset : Int64
  /// v1+; -1 for the leader or when unknown.
  last_fetch_timestamp : Int64
  last_caught_up_timestamp : Int64
} derive(@debug.Debug)

///|
pub struct QuorumPartitionState {
  partition_index : Int
  error_code : Int
  error_message : String?
  leader_id : Int
  leader_epoch : Int
  high_watermark : Int64
  current_voters : Array[QuorumReplicaState]
  observers : Array[QuorumReplicaState]
} derive(@debug.Debug)

///|
pub struct QuorumTopicState {
  topic_name : String
  partitions : Array[QuorumPartitionState]
} derive(@debug.Debug)

///|
pub struct QuorumNode {
  node_id : Int
  /// (name, host, port) listeners of this controller.
  listeners : Array[(String, String, Int)]
} derive(@debug.Debug)

///|
pub struct AdminQuorumDescription {
  error_code : Int
  error_message : String?
  topics : Array[QuorumTopicState]
  /// v2+ quorum node/listener map (KIP-853).
  nodes : Array[QuorumNode]
} derive(@debug.Debug)

///|
fn decode_quorum_replica_state(d : @buf.Decoder) -> QuorumReplicaState raise {
  let replica_id = d.read_i32()
  let directory_id = Uuid::from_bytes(d.read_bytes(16))
  let log_end_offset = d.read_i64()
  let last_fetch = d.read_i64()
  let last_caught_up = d.read_i64()
  d.skip_tag_buffer()
  {
    replica_id,
    replica_directory_id: directory_id,
    log_end_offset,
    last_fetch_timestamp: last_fetch,
    last_caught_up_timestamp: last_caught_up,
  }
}

///|
/// Encode a DescribeQuorum v2 request body.
pub fn encode_describe_quorum_request(
  topics : Array[(String, Array[Int])],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(topics.length())
  for entry in topics {
    let (name, partitions) = entry
    body.write_compact_string(name)
    body.write_compact_len(partitions.length())
    for partition in partitions {
      body.write_i32(partition)
      body.write_tag_buffer()
    }
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DescribeQuorum v2 response body. Note: this API carries no
/// throttle_time_ms.
pub fn decode_describe_quorum_response(
  d : @buf.Decoder,
) -> AdminQuorumDescription raise {
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  let topics : Array[QuorumTopicState] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let topic_name = d.read_compact_string()
    let partitions : Array[QuorumPartitionState] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      let partition_index = d.read_i32()
      let partition_error = d.read_i16()
      let partition_message = d.read_compact_nullable_string()
      let leader_id = d.read_i32()
      let leader_epoch = d.read_i32()
      let high_watermark = d.read_i64()
      let voters : Array[QuorumReplicaState] = []
      let voter_count = d.read_compact_len()
      for _ in 0..<voter_count {
        voters.push(decode_quorum_replica_state(d))
      }
      let observers : Array[QuorumReplicaState] = []
      let observer_count = d.read_compact_len()
      for _ in 0..<observer_count {
        observers.push(decode_quorum_replica_state(d))
      }
      d.skip_tag_buffer()
      partitions.push({
        partition_index,
        error_code: partition_error,
        error_message: partition_message,
        leader_id,
        leader_epoch,
        high_watermark,
        current_voters: voters,
        observers,
      })
    }
    d.skip_tag_buffer()
    topics.push({ topic_name, partitions, })
  }
  let nodes : Array[QuorumNode] = []
  let node_count = d.read_compact_len()
  for _ in 0..<node_count {
    let node_id = d.read_i32()
    let listeners : Array[(String, String, Int)] = []
    let listener_count = d.read_compact_len()
    for _ in 0..<listener_count {
      let name = d.read_compact_string()
      let host = d.read_compact_string()
      let port = d.read_i16() & 0xFFFF // uint16 on the wire
      d.skip_tag_buffer()
      listeners.push((name, host, port))
    }
    d.skip_tag_buffer()
    nodes.push({ node_id, listeners, })
  }
  d.skip_tag_buffer()
  { error_code, error_message, topics, nodes, }
}

///|
pub async fn BrokerConnection::describe_quorum(
  self : BrokerConnection,
  topics : Array[(String, Array[Int])],
  timeout_ms? : Int = 30000,
) -> AdminQuorumDescription {
  let d = self.request(
    API_DESCRIBE_QUORUM,
    self.api_version(API_DESCRIBE_QUORUM),
    encode_describe_quorum_request(topics),
    timeout_ms~,
  )
  let result = decode_describe_quorum_response(d)
  result
}

///|
/// One finalized feature level update.
pub(all) struct FeatureUpdate {
  feature : String
  /// The new maximum version level; < 1 deletes the finalized feature.
  max_version_level : Int
  /// FEATURE_UPGRADE (1), FEATURE_SAFE_DOWNGRADE (2), or
  /// FEATURE_UNSAFE_DOWNGRADE (3).
  upgrade_type : Int
} derive(@debug.Debug)

///|
pub struct AdminUpdateFeaturesResult {
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
/// Encode an UpdateFeatures v2 request body.
pub fn encode_update_features_request(
  updates : Array[FeatureUpdate],
  timeout_ms : Int,
  validate_only : Bool,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_i32(timeout_ms)
  body.write_compact_len(updates.length())
  for update in updates {
    body.write_compact_string(update.feature)
    body.write_i16(update.max_version_level)
    body.write_i8(update.upgrade_type)
    body.write_tag_buffer()
  }
  body.write_bool(validate_only)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an UpdateFeatures v2 response body (v2 dropped the per-feature
/// results array).
pub fn decode_update_features_response(
  d : @buf.Decoder,
) -> (AdminUpdateFeaturesResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  d.skip_tag_buffer()
  ({ error_code, error_message, }, throttle)
}

///|
pub async fn BrokerConnection::update_features(
  self : BrokerConnection,
  updates : Array[FeatureUpdate],
  timeout_ms? : Int = 30000,
  validate_only? : Bool = false,
) -> AdminUpdateFeaturesResult {
  let d = self.request(
    API_UPDATE_FEATURES,
    self.api_version(API_UPDATE_FEATURES),
    encode_update_features_request(updates, timeout_ms, validate_only),
    timeout_ms~,
  )
  let (result, throttle) = decode_update_features_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// Describe the cluster: brokers, controller, cluster id. Served by any
/// broker; `include_fenced_brokers` only applies at v2.
pub async fn Admin::describe_cluster(
  self : Admin,
  include_authorized_operations? : Bool = false,
  include_fenced_brokers? : Bool = false,
) -> AdminClusterDescription {
  self.with_retries(
    fn(result : AdminClusterDescription) {
      admin_error_retriable(result.error_code)
    },
    async fn() {
      let conn = self.any_conn()
      conn.describe_cluster(
        include_authorized_operations~,
        include_fenced_brokers~,
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}

///|
/// Describe configs of the given resources (topics, brokers, broker
/// loggers, client-metrics, groups). Retriable per-resource errors
/// re-issue under the policy.
pub async fn Admin::describe_configs(
  self : Admin,
  resources : Array[ConfigResourceKey],
) -> Array[AdminConfigsResult] {
  self.with_retries(
    fn(results : Array[AdminConfigsResult]) {
      let mut retry = false
      for result in results {
        if admin_error_retriable(result.error_code) {
          retry = true
        }
      }
      retry
    },
    async fn() {
      let conn = self.any_conn()
      conn.describe_configs(resources, timeout_ms=self.request_timeout_ms)
    },
  )
}

///|
/// Apply incremental config updates (set/delete/append/subtract per
/// key), the alter path that preserves unknown configs.
pub async fn Admin::incremental_alter_configs(
  self : Admin,
  resources : Array[AlterableConfigResource],
  validate_only? : Bool = false,
) -> Array[AlterConfigsResult] {
  self.with_retries(
    fn(results : Array[AlterConfigsResult]) {
      let mut retry = false
      for result in results {
        if admin_error_retriable(result.error_code) {
          retry = true
        }
      }
      retry
    },
    async fn() {
      let conn = self.any_conn()
      conn.incremental_alter_configs(
        resources,
        timeout_ms=self.request_timeout_ms,
        validate_only~,
      )
    },
  )
}

///|
/// Legacy full-replace config alter: the given configs replace the
/// resource's dynamic config set. Prefer `incremental_alter_configs`.
pub async fn Admin::alter_configs(
  self : Admin,
  resources : Array[SettableConfigResource],
  validate_only? : Bool = false,
) -> Array[AlterConfigsResult] {
  self.with_retries(
    fn(results : Array[AlterConfigsResult]) {
      let mut retry = false
      for result in results {
        if admin_error_retriable(result.error_code) {
          retry = true
        }
      }
      retry
    },
    async fn() {
      let conn = self.any_conn()
      conn.alter_configs(
        resources,
        timeout_ms=self.request_timeout_ms,
        validate_only~,
      )
    },
  )
}

///|
/// List the broker's config resources, optionally filtered by type.
pub async fn Admin::list_config_resources(
  self : Admin,
  resource_types? : Array[Int] = [],
) -> ListConfigResourcesResult {
  self.with_retries(
    fn(result : ListConfigResourcesResult) {
      admin_error_retriable(result.error_code)
    },
    async fn() {
      let conn = self.any_conn()
      conn.list_config_resources(
        resource_types~,
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}

///|
/// Describe the broker log directories: per-dir partitions, sizes, lag,
/// volume capacity, and the cordon state. `topics` None asks for all.
pub async fn Admin::describe_log_dirs(
  self : Admin,
  topics? : Array[(String, Array[Int])]? = None,
) -> AdminLogDirs {
  self.with_retries(
    fn(result : AdminLogDirs) {
      let mut retry = admin_error_retriable(result.error_code)
      for dir in result.results {
        if admin_error_retriable(dir.error_code) {
          retry = true
        }
      }
      retry
    },
    async fn() {
      let conn = self.any_conn()
      conn.describe_log_dirs(topics~, timeout_ms=self.request_timeout_ms)
    },
  )
}

///|
/// Request leader elections (preferred or unclean) for the given topic
/// partitions; None elects for every partition.
pub async fn Admin::elect_leaders(
  self : Admin,
  election_type : Int,
  topic_partitions? : Array[(String, Array[Int])]? = None,
) -> AdminElectionResult {
  self.with_retries(
    fn(result : AdminElectionResult) {
      let mut retry = admin_error_retriable(result.error_code)
      for topic in result.results {
        for partition in topic.partitions {
          if admin_error_retriable(partition.error_code) {
            retry = true
          }
        }
      }
      retry
    },
    async fn() {
      let conn = self.any_conn()
      conn.elect_leaders(
        election_type,
        topic_partitions~,
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}

///|
/// Move partitions onto new replica sets; a None replica list cancels a
/// pending reassignment.
pub async fn Admin::alter_partition_reassignments(
  self : Admin,
  topics : Array[ReassignableTopic],
  allow_replication_factor_change? : Bool = true,
) -> AdminReassignmentResult {
  self.with_retries(
    fn(result : AdminReassignmentResult) {
      let mut retry = admin_error_retriable(result.error_code)
      for entry in result.responses {
        let (_, partitions) = entry
        for partition in partitions {
          if admin_error_retriable(partition.error_code) {
            retry = true
          }
        }
      }
      retry
    },
    async fn() {
      let conn = self.any_conn()
      conn.alter_partition_reassignments(
        topics,
        timeout_ms=self.request_timeout_ms,
        allow_replication_factor_change~,
      )
    },
  )
}

///|
/// List ongoing partition reassignments, optionally filtered by topic.
pub async fn Admin::list_partition_reassignments(
  self : Admin,
  topics? : Array[(String, Array[Int])]? = None,
) -> AdminOngoingReassignments {
  self.with_retries(
    fn(result : AdminOngoingReassignments) {
      admin_error_retriable(result.error_code)
    },
    async fn() {
      let conn = self.any_conn()
      conn.list_partition_reassignments(
        topics~,
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}

///|
/// Unregister a fenced broker from the cluster (controller-routed).
pub async fn Admin::unregister_broker(
  self : Admin,
  broker_id : Int,
) -> UnregisterBrokerResult {
  self.with_retries(
    fn(result : UnregisterBrokerResult) {
      admin_error_retriable(result.error_code)
    },
    async fn() {
      let conn = self.controller_conn()
      conn.unregister_broker(broker_id, timeout_ms=self.request_timeout_ms)
    },
  )
}

///|
/// Describe the metadata quorum: leader, high watermark, and voter
/// progress. Defaults to the cluster metadata partition.
pub async fn Admin::describe_quorum(
  self : Admin,
  topic? : String = "__cluster_metadata",
  partition? : Int = 0,
) -> AdminQuorumDescription {
  self.with_retries(
    fn(result : AdminQuorumDescription) {
      let mut retry = admin_error_retriable(result.error_code)
      for t in result.topics {
        for partition in t.partitions {
          if admin_error_retriable(partition.error_code) {
            retry = true
          }
        }
      }
      retry
    },
    async fn() {
      let conn = self.any_conn()
      conn.describe_quorum(
        [(topic, [partition])],
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}

///|
/// Update finalized feature levels (upgrade, safe/unsafe downgrade, or
/// delete with a level < 1).
pub async fn Admin::update_features(
  self : Admin,
  updates : Array[FeatureUpdate],
  validate_only? : Bool = false,
) -> AdminUpdateFeaturesResult {
  self.with_retries(
    fn(result : AdminUpdateFeaturesResult) {
      admin_error_retriable(result.error_code)
    },
    async fn() {
      let conn = self.controller_conn()
      conn.update_features(
        updates,
        timeout_ms=self.request_timeout_ms,
        validate_only~,
      )
    },
  )
}
