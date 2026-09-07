// Admin ACL operations (Phase 5): DescribeAcls v2-v3, CreateAcls v2-v3,
// and DeleteAcls v2-v3. Field order is pinned from the message schemas
// in the Kafka 4.3 tree under clients/src/main/resources/common/message/;
// the enum byte values from ResourceType.java, PatternType.java,
// AclOperation.java, and AclPermissionType.java. v3 is wire-identical
// to v2 (it only adds the USER resource type), so one shape serves both.
// Per-item error codes travel as values; the Admin retry policy
// re-issues retriable ones.

///|
/// ACL resource types (ResourceType byte values).
pub const ACL_RESOURCE_UNKNOWN : Int = 0

///|
pub const ACL_RESOURCE_ANY : Int = 1

///|
pub const ACL_RESOURCE_TOPIC : Int = 2

///|
pub const ACL_RESOURCE_GROUP : Int = 3

///|
pub const ACL_RESOURCE_CLUSTER : Int = 4

///|
pub const ACL_RESOURCE_TRANSACTIONAL_ID : Int = 5

///|
pub const ACL_RESOURCE_DELEGATION_TOKEN : Int = 6

///|
/// v3 and newer brokers.
pub const ACL_RESOURCE_USER : Int = 7

///|
/// Resource pattern types (PatternType byte values).
pub const ACL_PATTERN_UNKNOWN : Int = 0

///|
pub const ACL_PATTERN_ANY : Int = 1

///|
/// MATCH in filters; never describes a stored binding.
pub const ACL_PATTERN_MATCH : Int = 2

///|
pub const ACL_PATTERN_LITERAL : Int = 3

///|
pub const ACL_PATTERN_PREFIXED : Int = 4

///|
/// ACL operations (AclOperation byte values).
pub const ACL_OPERATION_UNKNOWN : Int = 0

///|
pub const ACL_OPERATION_ANY : Int = 1

///|
pub const ACL_OPERATION_ALL : Int = 2

///|
pub const ACL_OPERATION_READ : Int = 3

///|
pub const ACL_OPERATION_WRITE : Int = 4

///|
pub const ACL_OPERATION_CREATE : Int = 5

///|
pub const ACL_OPERATION_DELETE : Int = 6

///|
pub const ACL_OPERATION_ALTER : Int = 7

///|
pub const ACL_OPERATION_DESCRIBE : Int = 8

///|
pub const ACL_OPERATION_CLUSTER_ACTION : Int = 9

///|
pub const ACL_OPERATION_DESCRIBE_CONFIGS : Int = 10

///|
pub const ACL_OPERATION_ALTER_CONFIGS : Int = 11

///|
pub const ACL_OPERATION_IDEMPOTENT_WRITE : Int = 12

///|
pub const ACL_OPERATION_CREATE_TOKENS : Int = 13

///|
pub const ACL_OPERATION_DESCRIBE_TOKENS : Int = 14

///|
/// KIP-939 era; supported by 4.3 brokers.
pub const ACL_OPERATION_TWO_PHASE_COMMIT : Int = 15

///|
/// ACL permission types (AclPermissionType byte values).
pub const ACL_PERMISSION_UNKNOWN : Int = 0

///|
pub const ACL_PERMISSION_ANY : Int = 1

///|
pub const ACL_PERMISSION_DENY : Int = 2

///|
pub const ACL_PERMISSION_ALLOW : Int = 3

///|
/// One ACL binding: who may do what with which resource from where.
pub(all) struct AclBinding {
  resource_type : Int
  resource_name : String
  pattern_type : Int
  principal : String
  host : String
  operation : Int
  permission_type : Int
} derive(@debug.Debug)

///|
/// One ACL to create.
pub(all) struct AclCreation {
  resource_type : Int
  resource_name : String
  pattern_type : Int
  principal : String
  host : String
  operation : Int
  permission_type : Int
} derive(@debug.Debug)

///|
/// Filter describing which ACLs to list or delete: a None field matches
/// anything; ANY/MATCH enum values widen the match.
pub(all) struct AclFilter {
  resource_type : Int
  resource_name : String?
  pattern_type : Int
  principal : String?
  host : String?
  operation : Int
  permission_type : Int
} derive(@debug.Debug)

///|
fn encode_acl_creation(body : @buf.Encoder, creation : AclCreation) -> Unit {
  body.write_i8(creation.resource_type)
  body.write_compact_string(creation.resource_name)
  body.write_i8(creation.pattern_type)
  body.write_compact_string(creation.principal)
  body.write_compact_string(creation.host)
  body.write_i8(creation.operation)
  body.write_i8(creation.permission_type)
  body.write_tag_buffer()
}

///|
fn encode_acl_filter(body : @buf.Encoder, filter : AclFilter) -> Unit {
  body.write_i8(filter.resource_type)
  body.write_compact_nullable_string(filter.resource_name)
  body.write_i8(filter.pattern_type)
  body.write_compact_nullable_string(filter.principal)
  body.write_compact_nullable_string(filter.host)
  body.write_i8(filter.operation)
  body.write_i8(filter.permission_type)
  body.write_tag_buffer()
}

///|
fn decode_acl_binding(d : @buf.Decoder) -> AclBinding raise {
  let resource_type = d.read_i8()
  let resource_name = d.read_compact_string()
  let pattern_type = d.read_i8()
  let principal = d.read_compact_string()
  let host = d.read_compact_string()
  let operation = d.read_i8()
  let permission_type = d.read_i8()
  d.skip_tag_buffer()
  {
    resource_type,
    resource_name,
    pattern_type,
    principal,
    host,
    operation,
    permission_type,
  }
}

///|
/// One ACL creation result.
pub struct CreateAclsResult {
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
/// Encode a CreateAcls v2/v3 request body.
pub fn encode_create_acls_request(creations : Array[AclCreation]) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(creations.length())
  for creation in creations {
    encode_acl_creation(body, creation)
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a CreateAcls v2/v3 response body: one result per creation.
pub fn decode_create_acls_response(
  d : @buf.Decoder,
) -> (Array[CreateAclsResult], Int) raise {
  let throttle = d.read_i32()
  let out : Array[CreateAclsResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let error_code = d.read_i16()
    let error_message = d.read_compact_nullable_string()
    d.skip_tag_buffer()
    out.push({ error_code, error_message, })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::create_acls(
  self : BrokerConnection,
  creations : Array[AclCreation],
  timeout_ms? : Int = 30000,
) -> Array[CreateAclsResult] {
  let d = self.request(
    API_CREATE_ACLS,
    self.api_version(API_CREATE_ACLS),
    encode_create_acls_request(creations),
    timeout_ms~,
  )
  let (results, throttle) = decode_create_acls_response(d)
  self.note_throttle(throttle)
  results
}

///|
/// One ACL of a described resource.
pub(all) struct AclDescription {
  principal : String
  host : String
  operation : Int
  permission_type : Int
} derive(@debug.Debug)

///|
pub struct DescribedAclResource {
  resource_type : Int
  resource_name : String
  pattern_type : Int
  acls : Array[AclDescription]
} derive(@debug.Debug)

///|
pub struct DescribeAclsResult {
  error_code : Int
  error_message : String?
  resources : Array[DescribedAclResource]
} derive(@debug.Debug)

///|
/// Encode a DescribeAcls v2/v3 request body.
pub fn encode_describe_acls_request(filter : AclFilter) -> Bytes {
  let body = @buf.Encoder::new()
  encode_acl_filter(body, filter)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DescribeAcls v2/v3 response body.
pub fn decode_describe_acls_response(
  d : @buf.Decoder,
) -> (DescribeAclsResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  let resources : Array[DescribedAclResource] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let resource_type = d.read_i8()
    let resource_name = d.read_compact_string()
    let pattern_type = d.read_i8()
    let acls : Array[AclDescription] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      let principal = d.read_compact_string()
      let host = d.read_compact_string()
      let operation = d.read_i8()
      let permission_type = d.read_i8()
      d.skip_tag_buffer()
      acls.push({ principal, host, operation, permission_type, })
    }
    d.skip_tag_buffer()
    resources.push({ resource_type, resource_name, pattern_type, acls, })
  }
  d.skip_tag_buffer()
  ({ error_code, error_message, resources, }, throttle)
}

///|
pub async fn BrokerConnection::describe_acls(
  self : BrokerConnection,
  filter : AclFilter,
  timeout_ms? : Int = 30000,
) -> DescribeAclsResult {
  let d = self.request(
    API_DESCRIBE_ACLS,
    self.api_version(API_DESCRIBE_ACLS),
    encode_describe_acls_request(filter),
    timeout_ms~,
  )
  let (result, throttle) = decode_describe_acls_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// One ACL a delete filter matched, with its own deletion error.
pub struct DeletedAcl {
  error_code : Int
  error_message : String?
  binding : AclBinding
} derive(@debug.Debug)

///|
pub struct DeleteAclsFilterResult {
  error_code : Int
  error_message : String?
  matching_acls : Array[DeletedAcl]
} derive(@debug.Debug)

///|
/// Encode a DeleteAcls v2/v3 request body.
pub fn encode_delete_acls_request(filters : Array[AclFilter]) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(filters.length())
  for filter in filters {
    encode_acl_filter(body, filter)
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DeleteAcls v2/v3 response body: one result per filter, each
/// carrying the ACLs it matched and deleted.
pub fn decode_delete_acls_response(
  d : @buf.Decoder,
) -> (Array[DeleteAclsFilterResult], Int) raise {
  let throttle = d.read_i32()
  let out : Array[DeleteAclsFilterResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let error_code = d.read_i16()
    let error_message = d.read_compact_nullable_string()
    let matching_acls : Array[DeletedAcl] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      let acl_error = d.read_i16()
      let acl_message = d.read_compact_nullable_string()
      let binding = decode_acl_binding(d)
      matching_acls.push({
        error_code: acl_error,
        error_message: acl_message,
        binding,
      })
    }
    d.skip_tag_buffer()
    out.push({ error_code, error_message, matching_acls, })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::delete_acls(
  self : BrokerConnection,
  filters : Array[AclFilter],
  timeout_ms? : Int = 30000,
) -> Array[DeleteAclsFilterResult] {
  let d = self.request(
    API_DELETE_ACLS,
    self.api_version(API_DELETE_ACLS),
    encode_delete_acls_request(filters),
    timeout_ms~,
  )
  let (results, throttle) = decode_delete_acls_response(d)
  self.note_throttle(throttle)
  results
}

///|
/// List the ACLs matching the filter (an all-ANY filter with null
/// fields lists everything). Retriable errors re-issue under the policy.
pub async fn Admin::describe_acls(
  self : Admin,
  filter : AclFilter,
) -> DescribeAclsResult {
  self.with_retries(
    fn(result : DescribeAclsResult) { admin_error_retriable(result.error_code) },
    async fn() {
      let conn = self.any_conn()
      conn.describe_acls(filter, timeout_ms=self.request_timeout_ms)
    },
  )
}

///|
/// Create ACL bindings; one result per creation.
pub async fn Admin::create_acls(
  self : Admin,
  creations : Array[AclCreation],
) -> Array[CreateAclsResult] {
  self.with_retries(
    fn(results : Array[CreateAclsResult]) {
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
      conn.create_acls(creations, timeout_ms=self.request_timeout_ms)
    },
  )
}

///|
/// Delete every ACL matching each filter; one result per filter with
/// the matched bindings.
pub async fn Admin::delete_acls(
  self : Admin,
  filters : Array[AclFilter],
) -> Array[DeleteAclsFilterResult] {
  self.with_retries(
    fn(results : Array[DeleteAclsFilterResult]) {
      let mut retry = false
      for result in results {
        if admin_error_retriable(result.error_code) {
          retry = true
        }
        for acl in result.matching_acls {
          if admin_error_retriable(acl.error_code) {
            retry = true
          }
        }
      }
      retry
    },
    async fn() {
      let conn = self.any_conn()
      conn.delete_acls(filters, timeout_ms=self.request_timeout_ms)
    },
  )
}
