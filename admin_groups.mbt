// Admin client group-inspection operations (Phase 5): DescribeGroups v6
// and ConsumerGroupDescribe v1. Field order is pinned from
// DescribeGroupsRequest/Response.json and
// ConsumerGroupDescribeRequest/Response.json in the Kafka 4.3 tree under
// clients/src/main/resources/common/message/; the member-type byte values
// from ConsumerGroupDescribeResponse.json (KIP-1099). DescribeGroups v5 is
// the first flexible version and v6 (KIP-1043) adds the per-group
// ErrorMessage; ConsumerGroupDescribe is flexible from v0 and v1 adds the
// member type. Both are implemented at a single version, so every field the
// schemas gate on an older version is present and each codec reads one
// shape. Per-group error codes travel as values; the Admin retry policy
// re-issues retriable ones.
//
// The two calls cover the two group protocols, and neither substitutes for
// the other: DescribeGroups describes CLASSIC groups, while a KIP-848
// consumer group is not one — the coordinator's
// GroupMetadataManager.classicGroup raises GroupIdNotFoundException for any
// non-classic group, which v6 turns into error code GROUP_ID_NOT_FOUND (69)
// with group state "Dead". ConsumerGroupDescribe is the call that returns
// new-protocol members, with their epochs, subscriptions, and assignments.

///|
/// GROUP_ID_NOT_FOUND: what v6 reports for a group the coordinator does
/// not hold as a classic group (KIP-1043), including KIP-848 consumer
/// groups.
pub const GROUP_ID_NOT_FOUND : Int = 69

///|
/// The AuthorizedOperations sentinel a broker sends when the request did
/// not ask for authorized operations, or the group errored
/// (Integer.MIN_VALUE in the Java client).
pub const AUTHORIZED_OPERATIONS_NONE : Int = -2147483648

///|
/// One member of a described classic group.
pub struct AdminGroupMember {
  member_id : String
  /// The static-membership instance id (v4+), null for dynamic members.
  group_instance_id : String?
  client_id : String
  client_host : String
  /// The classic ConsumerProtocolSubscription payload. The broker only
  /// fills it while the group is Stable.
  member_metadata : Bytes
  /// The classic ConsumerProtocolAssignment payload. The broker only
  /// fills it while the group is Stable.
  member_assignment : Bytes
} derive(@debug.Debug)

///|
/// The member's subscribed topics, decoded from `member_metadata`. Empty
/// when the broker sent no metadata (any state but Stable) or the payload
/// is not the version-0 ConsumerProtocolSubscription this driver writes —
/// a custom protocol's metadata is opaque, so read the raw bytes.
pub fn AdminGroupMember::subscribed_topics(
  self : AdminGroupMember,
) -> Array[String] {
  if self.member_metadata.length() == 0 {
    return []
  }
  decode_consumer_protocol_subscription(self.member_metadata) catch {
    _ => []
  }
}

///|
/// The member's assigned partitions, decoded from `member_assignment`.
/// Empty under the same conditions as `subscribed_topics`.
pub fn AdminGroupMember::assigned_partitions(
  self : AdminGroupMember,
) -> Array[(String, Array[Int])] {
  if self.member_assignment.length() == 0 {
    return []
  }
  decode_consumer_protocol_assignment(self.member_assignment) catch {
    _ => []
  }
}

///|
/// One group's DescribeGroups result (error codes as values).
pub struct AdminGroupDescription {
  error_code : Int
  /// The broker's error message (v6, KIP-1043); null when there was none.
  error_message : String?
  group_id : String
  /// The state string: Stable, Empty, PreparingRebalance,
  /// CompletingRebalance, or Dead.
  group_state : String
  /// "consumer" for consumer groups, "connect" for Kafka Connect, and so
  /// on; the empty string when the coordinator has not settled one.
  protocol_type : String
  /// The settled protocol name (the classic assignor). Only filled while
  /// the group is Stable.
  protocol_data : String
  /// Always empty for a group in the Dead state.
  members : Array[AdminGroupMember]
  /// The authorized-operations bitfield, or None when the request did not
  /// ask for it (the broker's MIN_VALUE sentinel).
  authorized_operations : Int?
} derive(@debug.Debug)

///|
/// True when the broker could not describe the group because it is not a
/// classic group — the KIP-848 consumer-group case, where v6 reports
/// GROUP_ID_NOT_FOUND (69). Such a group needs ConsumerGroupDescribe.
pub fn AdminGroupDescription::is_not_found(
  self : AdminGroupDescription,
) -> Bool {
  self.error_code == GROUP_ID_NOT_FOUND
}

///|
/// Encode a DescribeGroups v6 request body.
pub fn encode_describe_groups_request(
  groups : Array[String],
  include_authorized_operations : Bool,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(groups.length())
  for group in groups {
    body.write_compact_string(group)
  }
  body.write_bool(include_authorized_operations)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DescribeGroups v6 response body: one description per
/// requested group, in request order.
pub fn decode_describe_groups_response(
  d : @buf.Decoder,
) -> (Array[AdminGroupDescription], Int) raise {
  let throttle = d.read_i32()
  let out : Array[AdminGroupDescription] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let error_code = d.read_i16()
    let error_message = d.read_compact_nullable_string()
    let group_id = d.read_compact_string()
    let group_state = d.read_compact_string()
    let protocol_type = d.read_compact_string()
    let protocol_data = d.read_compact_string()
    let members : Array[AdminGroupMember] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      let member_id = d.read_compact_string()
      let group_instance_id = d.read_compact_nullable_string()
      let client_id = d.read_compact_string()
      let client_host = d.read_compact_string()
      let member_metadata = d.read_bytes(d.read_compact_len())
      let member_assignment = d.read_bytes(d.read_compact_len())
      d.skip_tag_buffer()
      members.push({
        member_id,
        group_instance_id,
        client_id,
        client_host,
        member_metadata,
        member_assignment,
      })
    }
    // AuthorizedOperations trails the members inside the same struct, so no
    // tag buffer separates them; MIN_VALUE means "absent".
    let authorized = d.read_i32()
    d.skip_tag_buffer()
    out.push({
      error_code,
      error_message,
      group_id,
      group_state,
      protocol_type,
      protocol_data,
      members,
      authorized_operations: if authorized == AUTHORIZED_OPERATIONS_NONE {
        None
      } else {
        Some(authorized)
      },
    })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::describe_groups(
  self : BrokerConnection,
  groups : Array[String],
  include_authorized_operations? : Bool = false,
  timeout_ms? : Int = 30000,
) -> Array[AdminGroupDescription] {
  let d = self.request(
    API_DESCRIBE_GROUPS,
    self.api_version(API_DESCRIBE_GROUPS),
    encode_describe_groups_request(groups, include_authorized_operations),
    timeout_ms~,
  )
  let (result, throttle) = decode_describe_groups_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// Describe classic consumer groups: state, protocol, and members with
/// their subscriptions and assignments. One description per requested
/// group, in request order, with per-group error codes as values.
///
/// A KIP-848 consumer group comes back with `is_not_found()` true and
/// state "Dead" — DescribeGroups only describes classic groups; use
/// ConsumerGroupDescribe for the new protocol.
pub async fn Admin::describe_groups(
  self : Admin,
  groups : Array[String],
  include_authorized_operations? : Bool = false,
) -> Array[AdminGroupDescription] {
  self.with_retries(
    fn(results : Array[AdminGroupDescription]) {
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
      conn.describe_groups(
        groups,
        include_authorized_operations~,
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}

///|
/// MemberType values from ConsumerGroupDescribe v1 (KIP-1099): a group
/// migrating between protocols can hold both kinds at once.
pub const GROUP_MEMBER_TYPE_UNKNOWN : Int = -1

///|
pub const GROUP_MEMBER_TYPE_CLASSIC : Int = 0

///|
pub const GROUP_MEMBER_TYPE_CONSUMER : Int = 1

///|
/// One topic's partitions inside a member's assignment.
pub struct AdminAssignedTopicPartitions {
  topic_id : Uuid
  topic_name : String
  partitions : Array[Int]
} derive(@debug.Debug)

///|
/// One member of a KIP-848 consumer group.
pub struct AdminConsumerGroupMember {
  member_id : String
  /// The static-membership instance id, null for dynamic members.
  instance_id : String?
  rack_id : String?
  /// The epoch this member last reconciled at; behind the group epoch
  /// while it catches up.
  member_epoch : Int
  client_id : String
  client_host : String
  subscribed_topic_names : Array[String]
  /// The server-side regex subscription (KIP-848 v1 heartbeats), null
  /// when the member subscribed by topic list.
  subscribed_topic_regex : String?
  /// What the member is currently fetching from.
  assignment : Array[AdminAssignedTopicPartitions]
  /// What the broker intends it to fetch from once it reconciles; differs
  /// from `assignment` while the member epoch lags the assignment epoch.
  target_assignment : Array[AdminAssignedTopicPartitions]
  member_type : Int
} derive(@debug.Debug)

///|
/// One group's ConsumerGroupDescribe result (error codes as values).
pub struct AdminConsumerGroupDescription {
  error_code : Int
  error_message : String?
  group_id : String
  /// Empty, Stable, or Assigning.
  group_state : String
  group_epoch : Int
  /// The epoch of the metadata the target assignments were computed from;
  /// behind `group_epoch` while the broker recomputes.
  assignment_epoch : Int
  /// The selected server-side assignor.
  assignor_name : String
  members : Array[AdminConsumerGroupMember]
  /// The authorized-operations bitfield, or None when the request did not
  /// ask for it (the broker's MIN_VALUE sentinel).
  authorized_operations : Int?
} derive(@debug.Debug)

///|
/// Encode a ConsumerGroupDescribe v1 request body (wire-identical to v0).
pub fn encode_consumer_group_describe_request(
  group_ids : Array[String],
  include_authorized_operations : Bool,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(group_ids.length())
  for group_id in group_ids {
    body.write_compact_string(group_id)
  }
  body.write_bool(include_authorized_operations)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode the Assignment struct: a wrapper around a TopicPartitions array,
/// so each element and the wrapper itself carry a tag buffer. The wrapper
/// is flattened away — it holds nothing else.
fn decode_group_assignment(
  d : @buf.Decoder,
) -> Array[AdminAssignedTopicPartitions] raise {
  let out : Array[AdminAssignedTopicPartitions] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let topic_id = Uuid::from_bytes(d.read_bytes(16))
    let topic_name = d.read_compact_string()
    let partitions : Array[Int] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      partitions.push(d.read_i32())
    }
    d.skip_tag_buffer()
    out.push({ topic_id, topic_name, partitions, })
  }
  d.skip_tag_buffer()
  out
}

///|
/// Decode a ConsumerGroupDescribe v1 response body: one description per
/// requested group, in request order.
pub fn decode_consumer_group_describe_response(
  d : @buf.Decoder,
) -> (Array[AdminConsumerGroupDescription], Int) raise {
  let throttle = d.read_i32()
  let out : Array[AdminConsumerGroupDescription] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let error_code = d.read_i16()
    let error_message = d.read_compact_nullable_string()
    let group_id = d.read_compact_string()
    let group_state = d.read_compact_string()
    let group_epoch = d.read_i32()
    let assignment_epoch = d.read_i32()
    let assignor_name = d.read_compact_string()
    let members : Array[AdminConsumerGroupMember] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      let member_id = d.read_compact_string()
      let instance_id = d.read_compact_nullable_string()
      let rack_id = d.read_compact_nullable_string()
      let member_epoch = d.read_i32()
      let client_id = d.read_compact_string()
      let client_host = d.read_compact_string()
      let subscribed_topic_names : Array[String] = []
      let topics = d.read_compact_len()
      for _ in 0..<topics {
        subscribed_topic_names.push(d.read_compact_string())
      }
      let subscribed_topic_regex = d.read_compact_nullable_string()
      let assignment = decode_group_assignment(d)
      let target_assignment = decode_group_assignment(d)
      let member_type = d.read_i8()
      d.skip_tag_buffer()
      members.push({
        member_id,
        instance_id,
        rack_id,
        member_epoch,
        client_id,
        client_host,
        subscribed_topic_names,
        subscribed_topic_regex,
        assignment,
        target_assignment,
        member_type,
      })
    }
    let authorized = d.read_i32()
    d.skip_tag_buffer()
    out.push({
      error_code,
      error_message,
      group_id,
      group_state,
      group_epoch,
      assignment_epoch,
      assignor_name,
      members,
      authorized_operations: if authorized == AUTHORIZED_OPERATIONS_NONE {
        None
      } else {
        Some(authorized)
      },
    })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::describe_consumer_groups(
  self : BrokerConnection,
  group_ids : Array[String],
  include_authorized_operations? : Bool = false,
  timeout_ms? : Int = 30000,
) -> Array[AdminConsumerGroupDescription] {
  let d = self.request(
    API_CONSUMER_GROUP_DESCRIBE,
    self.api_version(API_CONSUMER_GROUP_DESCRIBE),
    encode_consumer_group_describe_request(
      group_ids, include_authorized_operations,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_consumer_group_describe_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// Describe KIP-848 consumer groups: state, epochs, assignor, and members
/// with their subscriptions, current assignments, and target assignments.
/// One description per requested group, in request order, with per-group
/// error codes as values.
///
/// This is the call for new-protocol groups; `describe_groups` reports
/// GROUP_ID_NOT_FOUND for them.
pub async fn Admin::describe_consumer_groups(
  self : Admin,
  group_ids : Array[String],
  include_authorized_operations? : Bool = false,
) -> Array[AdminConsumerGroupDescription] {
  self.with_retries(
    fn(results : Array[AdminConsumerGroupDescription]) {
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
      conn.describe_consumer_groups(
        group_ids,
        include_authorized_operations~,
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}
