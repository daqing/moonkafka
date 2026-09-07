// API version negotiation (plan decision D1). This file is the single
// source of truth for two things: which API key numbers and versions this
// driver implements (the matrix), and how a request picks its version
// against the ranges a broker advertised in its ApiVersions response.
//
// Root scope for now; folds into the protocol/ package split together
// with the per-API codecs.

///|
// API keys are pinned from the 4.3 protocol guide as each codec lands;
// they are stable across 4.x.
pub const API_PRODUCE : Int = 0

///|
pub const API_FETCH : Int = 1

///|
pub const API_OFFSET_COMMIT : Int = 8

///|
pub const API_OFFSET_FETCH : Int = 9

///|
pub const API_OFFSET_FOR_LEADER_EPOCH : Int = 23

///|
pub const API_CONSUMER_GROUP_HEARTBEAT : Int = 68

///|
pub const API_JOIN_GROUP : Int = 11

///|
pub const API_SYNC_GROUP : Int = 14

///|
pub const API_HEARTBEAT : Int = 12

///|
pub const API_LEAVE_GROUP : Int = 13

///|
pub const API_CREATE_TOPICS : Int = 19

///|
pub const API_DELETE_TOPICS : Int = 20

///|
pub const API_DELETE_RECORDS : Int = 21

///|
pub const API_CREATE_PARTITIONS : Int = 37

///|
pub const API_OFFSET_DELETE : Int = 47

///|
pub const API_LIST_OFFSETS : Int = 2

///|
pub const API_METADATA : Int = 3

///|
pub const API_FIND_COORDINATOR : Int = 10

///|
pub const API_INIT_PRODUCER_ID : Int = 22

///|
pub const API_ADD_PARTITIONS_TO_TXN : Int = 24

///|
pub const API_ADD_OFFSETS_TO_TXN : Int = 25

///|
pub const API_END_TXN : Int = 26

///|
pub const API_TXN_OFFSET_COMMIT : Int = 28

///|
pub const API_DESCRIBE_TOPIC_PARTITIONS : Int = 75

///|
pub const API_DESCRIBE_ACLS : Int = 29

///|
pub const API_CREATE_ACLS : Int = 30

///|
pub const API_DELETE_ACLS : Int = 31

///|
pub const API_DESCRIBE_CLIENT_QUOTAS : Int = 48

///|
pub const API_ALTER_CLIENT_QUOTAS : Int = 49

///|
pub const API_DESCRIBE_USER_SCRAM_CREDENTIALS : Int = 50

///|
pub const API_ALTER_USER_SCRAM_CREDENTIALS : Int = 51

///|
pub const API_DESCRIBE_CONFIGS : Int = 32

///|
pub const API_ALTER_CONFIGS : Int = 33

///|
pub const API_INCREMENTAL_ALTER_CONFIGS : Int = 44

///|
pub const API_LIST_CONFIG_RESOURCES : Int = 74

///|
pub const API_DESCRIBE_LOG_DIRS : Int = 35

///|
pub const API_ELECT_LEADERS : Int = 43

///|
pub const API_ALTER_PARTITION_REASSIGNMENTS : Int = 45

///|
pub const API_LIST_PARTITION_REASSIGNMENTS : Int = 46

///|
pub const API_DESCRIBE_CLUSTER : Int = 60

///|
pub const API_UNREGISTER_BROKER : Int = 64

///|
pub const API_DESCRIBE_QUORUM : Int = 55

///|
pub const API_UPDATE_FEATURES : Int = 57

///|
pub const API_SASL_HANDSHAKE : Int = 17

///|
pub const API_API_VERSIONS : Int = 18

///|
pub const API_SASL_AUTHENTICATE : Int = 36

///|
// The handshake's own version cannot itself be negotiated: it is the
// newest version every 4.0+ broker accepts.
const API_VERSIONS_VERSION : Int = 3

///|
/// One row of the driver's version matrix: the contiguous range of
/// versions whose codecs are implemented for one API.
pub(all) struct ApiRange {
  key : Int
  name : String
  min : Int
  max : Int
} derive(@debug.Debug)

///|
/// The driver's version matrix. Grow a row's range only when codecs for
/// the new versions exist — `pick` may return any version inside a row,
/// and the encoders/decoders must be able to handle what it returns.
pub fn driver_api_range(api_key : Int) -> ApiRange? {
  match api_key {
    API_PRODUCE =>
      Some({ key: API_PRODUCE, name: "Produce", min: 12, max: 13, })
    API_FETCH => Some({ key: API_FETCH, name: "Fetch", min: 12, max: 16, })
    // OffsetCommit/OffsetFetch v10 address topics by id (KIP-848); v8/v9
    // keep the classic wire shape.
    API_OFFSET_COMMIT =>
      Some({ key: API_OFFSET_COMMIT, name: "OffsetCommit", min: 8, max: 10, })
    API_OFFSET_FETCH =>
      Some({ key: API_OFFSET_FETCH, name: "OffsetFetch", min: 8, max: 10, })
    API_OFFSET_FOR_LEADER_EPOCH =>
      Some({
        key: API_OFFSET_FOR_LEADER_EPOCH,
        name: "OffsetForLeaderEpoch",
        min: 4,
        max: 4,
      })
    // v1 carries SubscribedTopicRegex (KIP-848 GA).
    API_CONSUMER_GROUP_HEARTBEAT =>
      Some({
        key: API_CONSUMER_GROUP_HEARTBEAT,
        name: "ConsumerGroupHeartbeat",
        min: 0,
        max: 1,
      })
    // The classic group coordination quartet: JoinGroup v9 (reason
    // field), SyncGroup v5 (protocol name/echo), Heartbeat v4 and
    // LeaveGroup v5 (member identity array).
    API_JOIN_GROUP =>
      Some({ key: API_JOIN_GROUP, name: "JoinGroup", min: 9, max: 9, })
    API_SYNC_GROUP =>
      Some({ key: API_SYNC_GROUP, name: "SyncGroup", min: 5, max: 5, })
    API_HEARTBEAT =>
      Some({ key: API_HEARTBEAT, name: "Heartbeat", min: 4, max: 4, })
    API_LEAVE_GROUP =>
      Some({ key: API_LEAVE_GROUP, name: "LeaveGroup", min: 5, max: 5, })
    // Topic admin ops; v7 echoes topic ids, DeleteTopics v6 deletes by
    // id (topic-id-first, KIP-516).
    API_CREATE_TOPICS =>
      Some({ key: API_CREATE_TOPICS, name: "CreateTopics", min: 7, max: 7, })
    API_DELETE_TOPICS =>
      Some({ key: API_DELETE_TOPICS, name: "DeleteTopics", min: 6, max: 6, })
    API_CREATE_PARTITIONS =>
      Some({
        key: API_CREATE_PARTITIONS,
        name: "CreatePartitions",
        min: 3,
        max: 3,
      })
    API_DELETE_RECORDS =>
      Some({ key: API_DELETE_RECORDS, name: "DeleteRecords", min: 2, max: 2, })
    // OffsetDelete v0 is the one non-flexible admin API in the set.
    API_OFFSET_DELETE =>
      Some({ key: API_OFFSET_DELETE, name: "OffsetDelete", min: 0, max: 0, })
    API_LIST_OFFSETS =>
      Some({ key: API_LIST_OFFSETS, name: "ListOffsets", min: 10, max: 11, })
    API_METADATA =>
      Some({ key: API_METADATA, name: "Metadata", min: 12, max: 13, })
    API_FIND_COORDINATOR =>
      Some({
        key: API_FIND_COORDINATOR,
        name: "FindCoordinator",
        min: 4,
        max: 4,
      })
    API_INIT_PRODUCER_ID =>
      Some({
        key: API_INIT_PRODUCER_ID,
        name: "InitProducerId",
        min: 5,
        max: 5,
      })
    // AddPartitionsToTxn v3: the client shape (v4+ is the broker-batch
    // API of KIP-890, unused by producers).
    API_ADD_PARTITIONS_TO_TXN =>
      Some({
        key: API_ADD_PARTITIONS_TO_TXN,
        name: "AddPartitionsToTxn",
        min: 3,
        max: 3,
      })
    API_ADD_OFFSETS_TO_TXN =>
      Some({
        key: API_ADD_OFFSETS_TO_TXN,
        name: "AddOffsetsToTxn",
        min: 4,
        max: 4,
      })
    API_END_TXN => Some({ key: API_END_TXN, name: "EndTxn", min: 5, max: 5, })
    // TxnOffsetCommit v4: v5 is wire-identical and the schema caps
    // non-2PC transactional clients at v4.
    API_TXN_OFFSET_COMMIT =>
      Some({
        key: API_TXN_OFFSET_COMMIT,
        name: "TxnOffsetCommit",
        min: 4,
        max: 4,
      })
    API_DESCRIBE_TOPIC_PARTITIONS =>
      Some({
        key: API_DESCRIBE_TOPIC_PARTITIONS,
        name: "DescribeTopicPartitions",
        min: 0,
        max: 0,
      })
    // ACL admin: v1 added pattern types, v2 is the first flexible
    // version, v3 only adds the USER resource type (same wire shape).
    API_DESCRIBE_ACLS =>
      Some({ key: API_DESCRIBE_ACLS, name: "DescribeAcls", min: 2, max: 3, })
    API_CREATE_ACLS =>
      Some({ key: API_CREATE_ACLS, name: "CreateAcls", min: 2, max: 3, })
    API_DELETE_ACLS =>
      Some({ key: API_DELETE_ACLS, name: "DeleteAcls", min: 2, max: 3, })
    // Quota admin: v0 is the pre-flexible shape, v1 enables flexible
    // versions; both wire shapes are implemented.
    API_DESCRIBE_CLIENT_QUOTAS =>
      Some({
        key: API_DESCRIBE_CLIENT_QUOTAS,
        name: "DescribeClientQuotas",
        min: 0,
        max: 1,
      })
    API_ALTER_CLIENT_QUOTAS =>
      Some({
        key: API_ALTER_CLIENT_QUOTAS,
        name: "AlterClientQuotas",
        min: 0,
        max: 1,
      })
    // SCRAM credential admin is v0-only, flexible from the start.
    API_DESCRIBE_USER_SCRAM_CREDENTIALS =>
      Some({
        key: API_DESCRIBE_USER_SCRAM_CREDENTIALS,
        name: "DescribeUserScramCredentials",
        min: 0,
        max: 0,
      })
    API_ALTER_USER_SCRAM_CREDENTIALS =>
      Some({
        key: API_ALTER_USER_SCRAM_CREDENTIALS,
        name: "AlterUserScramCredentials",
        min: 0,
        max: 0,
      })
    // DescribeConfigs v4 is the first flexible version and every 4.0+
    // broker accepts it; v1-v3 stay non-flexible and unimplemented.
    API_DESCRIBE_CONFIGS =>
      Some({
        key: API_DESCRIBE_CONFIGS,
        name: "DescribeConfigs",
        min: 4,
        max: 4,
      })
    // AlterConfigs v2 is the first flexible version; v0/v1 are
    // pre-flexible legacy shapes.
    API_ALTER_CONFIGS =>
      Some({ key: API_ALTER_CONFIGS, name: "AlterConfigs", min: 2, max: 2, })
    // IncrementalAlterConfigs v1 is the first flexible version.
    API_INCREMENTAL_ALTER_CONFIGS =>
      Some({
        key: API_INCREMENTAL_ALTER_CONFIGS,
        name: "IncrementalAlterConfigs",
        min: 1,
        max: 1,
      })
    // v1 filters by resource type (KIP-1142); v0 only lists client
    // metrics resources on older brokers.
    API_LIST_CONFIG_RESOURCES =>
      Some({
        key: API_LIST_CONFIG_RESOURCES,
        name: "ListConfigResources",
        min: 0,
        max: 1,
      })
    // v2 is the first flexible version; v3+ only add response fields,
    // decoded per version up to v5 (is_cordoned, KIP-1066).
    API_DESCRIBE_LOG_DIRS =>
      Some({
        key: API_DESCRIBE_LOG_DIRS,
        name: "DescribeLogDirs",
        min: 2,
        max: 5,
      })
    API_ELECT_LEADERS =>
      Some({ key: API_ELECT_LEADERS, name: "ElectLeaders", min: 2, max: 2, })
    // v1 toggles AllowReplicationFactorChange; both shapes are decoded.
    API_ALTER_PARTITION_REASSIGNMENTS =>
      Some({
        key: API_ALTER_PARTITION_REASSIGNMENTS,
        name: "AlterPartitionReassignments",
        min: 0,
        max: 1,
      })
    API_LIST_PARTITION_REASSIGNMENTS =>
      Some({
        key: API_LIST_PARTITION_REASSIGNMENTS,
        name: "ListPartitionReassignments",
        min: 0,
        max: 0,
      })
    // v1 adds endpoint_type (KIP-919), v2 fenced brokers (KIP-1073).
    API_DESCRIBE_CLUSTER =>
      Some({
        key: API_DESCRIBE_CLUSTER,
        name: "DescribeCluster",
        min: 0,
        max: 2,
      })
    API_UNREGISTER_BROKER =>
      Some({
        key: API_UNREGISTER_BROKER,
        name: "UnregisterBroker",
        min: 0,
        max: 0,
      })
    API_DESCRIBE_QUORUM =>
      Some({ key: API_DESCRIBE_QUORUM, name: "DescribeQuorum", min: 2, max: 2, })
    // v2 keeps the request (validate_only + upgrade_type) but drops the
    // per-feature response results.
    API_UPDATE_FEATURES =>
      Some({ key: API_UPDATE_FEATURES, name: "UpdateFeatures", min: 2, max: 2, })
    API_API_VERSIONS =>
      Some({ key: API_API_VERSIONS, name: "ApiVersions", min: 3, max: 3, })
    API_SASL_HANDSHAKE =>
      Some({ key: API_SASL_HANDSHAKE, name: "SaslHandshake", min: 1, max: 1, })
    API_SASL_AUTHENTICATE =>
      Some({
        key: API_SASL_AUTHENTICATE,
        name: "SaslAuthenticate",
        min: 2,
        max: 2,
      })
    _ => None
  }
}

///|
/// The version ranges a broker advertised in its ApiVersions response.
pub struct BrokerVersions {
  ranges : Map[Int, (Int, Int)]
}

///|
/// Highest version common to the driver range [dmin, dmax] capped by
/// `want`, and the broker range [bmin, bmax]. None when they do not
/// intersect.
fn pick_common(
  dmin : Int,
  dmax : Int,
  want : Int,
  bmin : Int,
  bmax : Int,
) -> Int? {
  let top = Int::min(want, dmax)
  let bottom = Int::max(dmin, bmin)
  let upper = Int::min(top, bmax)
  if upper >= bottom {
    Some(upper)
  } else {
    None
  }
}

///|
/// Choose the request version for `api_key`: the highest version common
/// to the driver's implemented range and this broker's advertised range,
/// capped at `want` (the driver's ceiling when omitted). Raises when the
/// broker does not advertise the API at all, or when no version overlaps —
/// the telltale of a pre-4.0 broker.
pub fn BrokerVersions::pick(
  self : BrokerVersions,
  api_key : Int,
  want? : Int? = None,
) -> Int raise {
  guard driver_api_range(api_key) is Some(row) else {
    raise ProtocolError::ProtocolError(
      "moonkafka implements no version of API key \{api_key}",
    )
  }
  guard self.ranges.get(api_key) is Some((bmin, bmax)) else {
    raise ProtocolError::ProtocolError(
      "broker does not advertise API key \{api_key} (\{row.name})",
    )
  }
  let want = want.unwrap_or(row.max)
  match pick_common(row.min, row.max, want, bmin, bmax) {
    Some(version) => version
    None =>
      raise ProtocolError::ProtocolError(
        "no common \{row.name} version: broker supports \{bmin}-\{bmax}, moonkafka implements \{row.min}-\{row.max}; only Kafka 4.x+ brokers are supported",
      )
  }
}

///|
/// The broker's advertised range for one API, if it advertises the key.
pub fn BrokerVersions::range(
  self : BrokerVersions,
  api_key : Int,
) -> (Int, Int)? {
  self.ranges.get(api_key)
}

///|
/// Negotiated request version for one API on this connection: the highest
/// version common to the driver matrix and the ranges the broker
/// advertised at handshake time. Raises when the handshake has not run on
/// this connection.
pub fn BrokerConnection::api_version(
  self : BrokerConnection,
  api_key : Int,
  want? : Int? = None,
) -> Int raise {
  match self.versions {
    Some(versions) => versions.pick(api_key, want~)
    None =>
      raise ProtocolError::ProtocolError(
        "API versions have not been negotiated on this connection",
      )
  }
}
