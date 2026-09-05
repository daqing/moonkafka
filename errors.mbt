// Broker error codes from the Kafka 4.3 protocol error table
// (kafka.apache.org/43 protocol guide), cross-checked against
// Errors.java. Generated mechanically from the published 4.3 table;
// codes beyond it fall back to UNKNOWN, so later additions are safe.

///|
/// An error code reported by a broker in a response. `message` carries
/// call-site context (which request, which topic/partition) on top of
/// the protocol-level code.
pub(all) suberror BrokerError {
  BrokerError(Int, String)
}

///|
/// SASL negotiation or authentication failure. The connection is closed.
pub(all) suberror SaslError {
  SaslError(String)
}

///|
/// Transport-level failure. The affected connection has been closed when
/// either variant is raised; callers reconnect rather than retry in place.
pub(all) suberror TransportError {
  ConnectionClosed(String)
  RequestTimeout(String)
}

///|
/// Protocol name for the error code, e.g. "OFFSET_OUT_OF_RANGE".
pub fn BrokerError::name(self : BrokerError) -> String {
  let BrokerError(code, _) = self
  error_name(code)
}

///|
/// Whether the protocol marks this error as retriable.
pub fn BrokerError::is_retriable(self : BrokerError) -> Bool {
  let BrokerError(code, _) = self
  error_retriable(code)
}

///|
/// Protocol name for a broker error code; unknown codes render as
/// "UNKNOWN(code)".
pub fn error_name(code : Int) -> String {
  match error_codes.get(code) {
    Some((name, _)) => name
    None => "UNKNOWN(\{code})"
  }
}

///|
/// Whether the protocol marks the error code as retriable; unknown
/// codes are treated as not retriable.
pub fn error_retriable(code : Int) -> Bool {
  match error_codes.get(code) {
    Some((_, retriable)) => retriable
    None => false
  }
}

///|
let error_codes : Map[Int, (String, Bool)] = Map([
  (-1, ("UNKNOWN_SERVER_ERROR", false)),
  (0, ("NONE", false)),
  (1, ("OFFSET_OUT_OF_RANGE", false)),
  (2, ("CORRUPT_MESSAGE", true)),
  (3, ("UNKNOWN_TOPIC_OR_PARTITION", true)),
  (4, ("INVALID_FETCH_SIZE", false)),
  (5, ("LEADER_NOT_AVAILABLE", true)),
  (6, ("NOT_LEADER_OR_FOLLOWER", true)),
  (7, ("REQUEST_TIMED_OUT", true)),
  (8, ("BROKER_NOT_AVAILABLE", false)),
  (9, ("REPLICA_NOT_AVAILABLE", true)),
  (10, ("MESSAGE_TOO_LARGE", false)),
  (11, ("STALE_CONTROLLER_EPOCH", false)),
  (12, ("OFFSET_METADATA_TOO_LARGE", false)),
  (13, ("NETWORK_EXCEPTION", true)),
  (14, ("COORDINATOR_LOAD_IN_PROGRESS", true)),
  (15, ("COORDINATOR_NOT_AVAILABLE", true)),
  (16, ("NOT_COORDINATOR", true)),
  (17, ("INVALID_TOPIC_EXCEPTION", false)),
  (18, ("RECORD_LIST_TOO_LARGE", false)),
  (19, ("NOT_ENOUGH_REPLICAS", true)),
  (20, ("NOT_ENOUGH_REPLICAS_AFTER_APPEND", true)),
  (21, ("INVALID_REQUIRED_ACKS", false)),
  (22, ("ILLEGAL_GENERATION", false)),
  (23, ("INCONSISTENT_GROUP_PROTOCOL", false)),
  (24, ("INVALID_GROUP_ID", false)),
  (25, ("UNKNOWN_MEMBER_ID", false)),
  (26, ("INVALID_SESSION_TIMEOUT", false)),
  (27, ("REBALANCE_IN_PROGRESS", false)),
  (28, ("INVALID_COMMIT_OFFSET_SIZE", false)),
  (29, ("TOPIC_AUTHORIZATION_FAILED", false)),
  (30, ("GROUP_AUTHORIZATION_FAILED", false)),
  (31, ("CLUSTER_AUTHORIZATION_FAILED", false)),
  (32, ("INVALID_TIMESTAMP", false)),
  (33, ("UNSUPPORTED_SASL_MECHANISM", false)),
  (34, ("ILLEGAL_SASL_STATE", false)),
  (35, ("UNSUPPORTED_VERSION", false)),
  (36, ("TOPIC_ALREADY_EXISTS", false)),
  (37, ("INVALID_PARTITIONS", false)),
  (38, ("INVALID_REPLICATION_FACTOR", false)),
  (39, ("INVALID_REPLICA_ASSIGNMENT", false)),
  (40, ("INVALID_CONFIG", false)),
  (41, ("NOT_CONTROLLER", true)),
  (42, ("INVALID_REQUEST", false)),
  (43, ("UNSUPPORTED_FOR_MESSAGE_FORMAT", false)),
  (44, ("POLICY_VIOLATION", false)),
  (45, ("OUT_OF_ORDER_SEQUENCE_NUMBER", false)),
  (46, ("DUPLICATE_SEQUENCE_NUMBER", false)),
  (47, ("INVALID_PRODUCER_EPOCH", false)),
  (48, ("INVALID_TXN_STATE", false)),
  (49, ("INVALID_PRODUCER_ID_MAPPING", false)),
  (50, ("INVALID_TRANSACTION_TIMEOUT", false)),
  (51, ("CONCURRENT_TRANSACTIONS", true)),
  (52, ("TRANSACTION_COORDINATOR_FENCED", false)),
  (53, ("TRANSACTIONAL_ID_AUTHORIZATION_FAILED", false)),
  (54, ("SECURITY_DISABLED", false)),
  (55, ("OPERATION_NOT_ATTEMPTED", false)),
  (56, ("KAFKA_STORAGE_ERROR", true)),
  (57, ("LOG_DIR_NOT_FOUND", false)),
  (58, ("SASL_AUTHENTICATION_FAILED", false)),
  (59, ("UNKNOWN_PRODUCER_ID", false)),
  (60, ("REASSIGNMENT_IN_PROGRESS", false)),
  (61, ("DELEGATION_TOKEN_AUTH_DISABLED", false)),
  (62, ("DELEGATION_TOKEN_NOT_FOUND", false)),
  (63, ("DELEGATION_TOKEN_OWNER_MISMATCH", false)),
  (64, ("DELEGATION_TOKEN_REQUEST_NOT_ALLOWED", false)),
  (65, ("DELEGATION_TOKEN_AUTHORIZATION_FAILED", false)),
  (66, ("DELEGATION_TOKEN_EXPIRED", false)),
  (67, ("INVALID_PRINCIPAL_TYPE", false)),
  (68, ("NON_EMPTY_GROUP", false)),
  (69, ("GROUP_ID_NOT_FOUND", false)),
  (70, ("FETCH_SESSION_ID_NOT_FOUND", true)),
  (71, ("INVALID_FETCH_SESSION_EPOCH", true)),
  (72, ("LISTENER_NOT_FOUND", true)),
  (73, ("TOPIC_DELETION_DISABLED", false)),
  (74, ("FENCED_LEADER_EPOCH", true)),
  (75, ("UNKNOWN_LEADER_EPOCH", true)),
  (76, ("UNSUPPORTED_COMPRESSION_TYPE", false)),
  (77, ("STALE_BROKER_EPOCH", false)),
  (78, ("OFFSET_NOT_AVAILABLE", true)),
  (79, ("MEMBER_ID_REQUIRED", false)),
  (80, ("PREFERRED_LEADER_NOT_AVAILABLE", true)),
  (81, ("GROUP_MAX_SIZE_REACHED", false)),
  (82, ("FENCED_INSTANCE_ID", false)),
  (83, ("ELIGIBLE_LEADERS_NOT_AVAILABLE", true)),
  (84, ("ELECTION_NOT_NEEDED", true)),
  (85, ("NO_REASSIGNMENT_IN_PROGRESS", false)),
  (86, ("GROUP_SUBSCRIBED_TO_TOPIC", false)),
  (87, ("INVALID_RECORD", false)),
  (88, ("UNSTABLE_OFFSET_COMMIT", true)),
  (89, ("THROTTLING_QUOTA_EXCEEDED", true)),
  (90, ("PRODUCER_FENCED", false)),
  (91, ("RESOURCE_NOT_FOUND", false)),
  (92, ("DUPLICATE_RESOURCE", false)),
  (93, ("UNACCEPTABLE_CREDENTIAL", false)),
  (94, ("INCONSISTENT_VOTER_SET", false)),
  (95, ("INVALID_UPDATE_VERSION", false)),
  (96, ("FEATURE_UPDATE_FAILED", false)),
  (97, ("PRINCIPAL_DESERIALIZATION_FAILURE", false)),
  (98, ("SNAPSHOT_NOT_FOUND", false)),
  (99, ("POSITION_OUT_OF_RANGE", false)),
  (100, ("UNKNOWN_TOPIC_ID", true)),
  (101, ("DUPLICATE_BROKER_REGISTRATION", false)),
  (102, ("BROKER_ID_NOT_REGISTERED", false)),
  (103, ("INCONSISTENT_TOPIC_ID", true)),
  (104, ("INCONSISTENT_CLUSTER_ID", false)),
  (105, ("TRANSACTIONAL_ID_NOT_FOUND", false)),
  (106, ("FETCH_SESSION_TOPIC_ID_ERROR", true)),
  (107, ("INELIGIBLE_REPLICA", false)),
  (108, ("NEW_LEADER_ELECTED", false)),
  (109, ("OFFSET_MOVED_TO_TIERED_STORAGE", false)),
  (110, ("FENCED_MEMBER_EPOCH", false)),
  (111, ("UNRELEASED_INSTANCE_ID", false)),
  (112, ("UNSUPPORTED_ASSIGNOR", false)),
  (113, ("STALE_MEMBER_EPOCH", false)),
  (114, ("MISMATCHED_ENDPOINT_TYPE", false)),
  (115, ("UNSUPPORTED_ENDPOINT_TYPE", false)),
  (116, ("UNKNOWN_CONTROLLER_ID", false)),
  (117, ("UNKNOWN_SUBSCRIPTION_ID", false)),
  (118, ("TELEMETRY_TOO_LARGE", false)),
  (119, ("INVALID_REGISTRATION", false)),
  (120, ("TRANSACTION_ABORTABLE", false)),
  (121, ("INVALID_RECORD_STATE", false)),
  (122, ("SHARE_SESSION_NOT_FOUND", true)),
  (123, ("INVALID_SHARE_SESSION_EPOCH", true)),
  (124, ("FENCED_STATE_EPOCH", false)),
  (125, ("INVALID_VOTER_KEY", false)),
  (126, ("DUPLICATE_VOTER", false)),
  (127, ("VOTER_NOT_FOUND", false)),
  (128, ("INVALID_REGULAR_EXPRESSION", false)),
  (129, ("REBOOTSTRAP_REQUIRED", false)),
  (130, ("STREAMS_INVALID_TOPOLOGY", false)),
  (131, ("STREAMS_INVALID_TOPOLOGY_EPOCH", false)),
  (132, ("STREAMS_TOPOLOGY_FENCED", false)),
  (133, ("SHARE_SESSION_LIMIT_REACHED", true)),
])
