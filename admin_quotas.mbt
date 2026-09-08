// Admin client quota and SCRAM credential operations (Phase 5):
// DescribeClientQuotas v0-v1, AlterClientQuotas v0-v1,
// DescribeUserScramCredentials v0, and AlterUserScramCredentials v0.
// Field order is pinned from the message schemas in the Kafka 4.3 tree
// under clients/src/main/resources/common/message/; the SCRAM mechanism
// byte values from ScramMechanism.java, the quota match types from the
// DescribeClientQuotas schema ({0 = exact name, 1 = default name,
// 2 = any specified name}), and the entity type strings from
// ClientQuotaEntity.java. The quota APIs span the flexible boundary
// (v1 enables flexible versions), so their codecs shape strings,
// arrays, and tag buffers by version; the SCRAM APIs are flexible v0
// only. Per-item error codes travel as values; the Admin retry policy
// re-issues retriable ones.

///|
/// Quota entity types (ClientQuotaEntity.java).
pub const QUOTA_ENTITY_USER : String = "user"

///|
pub const QUOTA_ENTITY_CLIENT_ID : String = "client-id"

///|
pub const QUOTA_ENTITY_IP : String = "ip"

///|
/// Quota filter match types (DescribeClientQuotas MatchType values).
pub const QUOTA_MATCH_EXACT : Int = 0

///|
pub const QUOTA_MATCH_DEFAULT : Int = 1

///|
pub const QUOTA_MATCH_ANY : Int = 2

///|
/// SCRAM mechanisms (ScramMechanism.java byte values).
pub const SCRAM_MECHANISM_SHA_256 : Int = 1

///|
pub const SCRAM_MECHANISM_SHA_512 : Int = 2

///|
/// A quota entity: type plus name (null = the default entity, e.g. the
/// cluster-wide default quota).
pub(all) struct QuotaEntity {
  entity_type : String
  entity_name : String?
} derive(@debug.Debug)

///|
/// One filter component: which entity type to match and how (exact
/// name, defaulted name, or any specified name; ANY matches the null
/// default entity too).
pub(all) struct ClientQuotaFilterComponent {
  entity_type : String
  match_type : Int
  /// The name to match, or None for the DEFAULT/ANY match styles.
  matches : String?
} derive(@debug.Debug)

///|
/// Which quota entities DescribeClientQuotas returns: every component
/// must match (AND); `strict` excludes entities that carry entity types
/// beyond the filtered ones.
pub(all) struct ClientQuotaFilter {
  components : Array[ClientQuotaFilterComponent]
  strict : Bool
} derive(@debug.Debug)

///|
/// One quota config value of a described entity (producer_byte_rate,
/// consumer_byte_rate, request_percentage, ...).
pub(all) struct ClientQuotaValue {
  key : String
  value : Double
} derive(@debug.Debug)

///|
/// The quotas configured on one entity.
pub struct ClientQuotasEntry {
  entity : Array[QuotaEntity]
  values : Array[ClientQuotaValue]
} derive(@debug.Debug)

///|
pub struct DescribeClientQuotasResult {
  error_code : Int
  error_message : String?
  entries : Array[ClientQuotasEntry]
} derive(@debug.Debug)

///|
/// One config key to set or remove on an entity.
pub(all) struct ClientQuotaOp {
  key : String
  /// The value to set; ignored when `remove` is true.
  value : Double
  remove : Bool
} derive(@debug.Debug)

///|
/// Alter the quota config of one entity.
pub(all) struct ClientQuotaAlteration {
  entity : Array[QuotaEntity]
  ops : Array[ClientQuotaOp]
} derive(@debug.Debug)

///|
pub struct AlterClientQuotasResult {
  error_code : Int
  error_message : String?
  entity : Array[QuotaEntity]
} derive(@debug.Debug)

///|
/// Mechanism and iteration count of a stored SCRAM credential.
pub struct ScramCredentialInfo {
  mechanism : Int
  iterations : Int
} derive(@debug.Debug)

///|
/// One user's SCRAM credentials as stored on the broker.
pub struct DescribedScramCredentials {
  user : String
  error_code : Int
  error_message : String?
  credential_infos : Array[ScramCredentialInfo]
} derive(@debug.Debug)

///|
pub struct DescribeUserScramResult {
  error_code : Int
  error_message : String?
  results : Array[DescribedScramCredentials]
} derive(@debug.Debug)

///|
/// Delete one SCRAM credential of a user (one deletion per mechanism).
pub(all) struct ScramCredentialDeletion {
  name : String
  mechanism : Int
} derive(@debug.Debug)

///|
/// Insert or replace a SCRAM credential. `salt` and `salted_password`
/// follow the wire format (SaltedPassword = Hi(password, salt,
/// iterations)); ScramCredentialUpsertion::from_password derives them
/// from a plaintext password.
pub(all) struct ScramCredentialUpsertion {
  name : String
  mechanism : Int
  iterations : Int
  salt : Bytes
  salted_password : Bytes
} derive(@debug.Debug)

///|
/// Build an upsertion from a plaintext password: the salt derives from
/// a hash of the user, mechanism, and wall clock (unique per call, like
/// the SCRAM client nonce), and the salted password is PBKDF2-HMAC per
/// RFC 5802. Brokers accept iteration counts between 4096 and 16384.
pub fn ScramCredentialUpsertion::from_password(
  name : String,
  mechanism : Int,
  password : Bytes,
  iterations? : Int = 4096,
) -> ScramCredentialUpsertion raise {
  let sha512 = mechanism == SCRAM_MECHANISM_SHA_512
  guard mechanism == SCRAM_MECHANISM_SHA_256 || sha512 else {
    raise SaslError("unknown SCRAM mechanism \{mechanism}")
  }
  let salt = scram_fixed_to_bytes(
    @crypto.sha256(@utf8.encode("\{name}:\{mechanism}:\{@async.now()}")),
  )
  let salted_password = scram_salted_password(
    sha512, password, salt, iterations,
  )
  { name, mechanism, iterations, salt, salted_password, }
}

///|
pub struct AlterUserScramResult {
  user : String
  error_code : Int
  error_message : String?
} derive(@debug.Debug)

///|
/// The quota APIs cross the flexible boundary at v1: strings, array
/// lengths, and tag buffers shape by version (v0 is the legacy
/// int16/int32 shape without tag buffers).
fn write_versioned_len(body : @buf.Encoder, n : Int, version : Int) -> Unit {
  if version >= 1 {
    body.write_compact_len(n)
  } else {
    body.write_i32(n)
  }
}

///|
fn write_versioned_string(
  body : @buf.Encoder,
  s : String,
  version : Int,
) -> Unit {
  if version >= 1 {
    body.write_compact_string(s)
  } else {
    let b = @utf8.encode(s)
    body.write_i16(b.length())
    body.write_bytes(b)
  }
}

///|
fn write_versioned_nullable_string(
  body : @buf.Encoder,
  s : String?,
  version : Int,
) -> Unit {
  if version >= 1 {
    body.write_compact_nullable_string(s)
  } else {
    body.write_nullable_string(s)
  }
}

///|
fn write_versioned_tag(body : @buf.Encoder, version : Int) -> Unit {
  if version >= 1 {
    body.write_tag_buffer()
  }
}

///|
fn read_versioned_len(d : @buf.Decoder, version : Int) -> Int raise {
  if version >= 1 {
    d.read_compact_len()
  } else {
    d.read_i32()
  }
}

///|
fn read_versioned_nullable_string(
  d : @buf.Decoder,
  version : Int,
) -> String? raise {
  if version >= 1 {
    d.read_compact_nullable_string()
  } else {
    let len = d.read_i16()
    if len < 0 {
      None
    } else {
      Some(@utf8.decode_lossy(d.read_bytes(len)[:]))
    }
  }
}

///|
fn read_versioned_string(d : @buf.Decoder, version : Int) -> String raise {
  match read_versioned_nullable_string(d, version) {
    Some(s) => s
    None => raise @buf.DecodeError::Malformed("unexpected null string")
  }
}

///|
fn skip_versioned_tag(d : @buf.Decoder, version : Int) -> Unit raise {
  if version >= 1 {
    d.skip_tag_buffer()
  }
}

///|
fn write_compact_bytes(body : @buf.Encoder, b : Bytes) -> Unit {
  body.write_compact_len(b.length())
  body.write_bytes(b)
}

///|
fn encode_quota_entity(
  body : @buf.Encoder,
  entity : Array[QuotaEntity],
  version : Int,
) -> Unit {
  write_versioned_len(body, entity.length(), version)
  for e in entity {
    write_versioned_string(body, e.entity_type, version)
    write_versioned_nullable_string(body, e.entity_name, version)
    write_versioned_tag(body, version)
  }
}

///|
fn decode_quota_entity(
  d : @buf.Decoder,
  version : Int,
) -> Array[QuotaEntity] raise {
  let entity : Array[QuotaEntity] = []
  let n = read_versioned_len(d, version)
  for _ in 0..<n {
    let entity_type = read_versioned_string(d, version)
    let entity_name = read_versioned_nullable_string(d, version)
    skip_versioned_tag(d, version)
    entity.push({ entity_type, entity_name, })
  }
  entity
}

///|
/// Encode a DescribeClientQuotas v0-v1 request body.
pub fn encode_describe_client_quotas_request(
  filter : ClientQuotaFilter,
  version : Int,
) -> Bytes {
  let body = @buf.Encoder::new()
  write_versioned_len(body, filter.components.length(), version)
  for component in filter.components {
    write_versioned_string(body, component.entity_type, version)
    body.write_i8(component.match_type)
    write_versioned_nullable_string(body, component.matches, version)
    write_versioned_tag(body, version)
  }
  body.write_bool(filter.strict)
  write_versioned_tag(body, version)
  body.to_bytes()
}

///|
/// Decode a DescribeClientQuotas v0-v1 response body. The entries array
/// is nullable (the broker sends null on errors); a null decodes as
/// empty.
pub fn decode_describe_client_quotas_response(
  version : Int,
  d : @buf.Decoder,
) -> (DescribeClientQuotasResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = read_versioned_nullable_string(d, version)
  let entries : Array[ClientQuotasEntry] = []
  let n = read_versioned_len(d, version)
  for _ in 0..<n {
    let entity = decode_quota_entity(d, version)
    let values : Array[ClientQuotaValue] = []
    let count = read_versioned_len(d, version)
    for _ in 0..<count {
      let key = read_versioned_string(d, version)
      let value = d.read_f64()
      skip_versioned_tag(d, version)
      values.push({ key, value, })
    }
    skip_versioned_tag(d, version)
    entries.push({ entity, values, })
  }
  skip_versioned_tag(d, version)
  ({ error_code, error_message, entries, }, throttle)
}

///|
pub async fn BrokerConnection::describe_client_quotas(
  self : BrokerConnection,
  filter : ClientQuotaFilter,
  timeout_ms? : Int = 30000,
) -> DescribeClientQuotasResult {
  let version = self.api_version(API_DESCRIBE_CLIENT_QUOTAS)
  let d = self.request_raw(
    API_DESCRIBE_CLIENT_QUOTAS,
    version,
    encode_describe_client_quotas_request(filter, version),
    timeout_ms~,
    flexible=version >= 1,
  )
  let (result, throttle) = decode_describe_client_quotas_response(version, d)
  self.note_throttle(throttle)
  result
}

///|
/// Encode an AlterClientQuotas v0-v1 request body.
pub fn encode_alter_client_quotas_request(
  entries : Array[ClientQuotaAlteration],
  validate_only : Bool,
  version : Int,
) -> Bytes {
  let body = @buf.Encoder::new()
  write_versioned_len(body, entries.length(), version)
  for entry in entries {
    encode_quota_entity(body, entry.entity, version)
    write_versioned_len(body, entry.ops.length(), version)
    for op in entry.ops {
      write_versioned_string(body, op.key, version)
      body.write_f64(op.value)
      body.write_bool(op.remove)
      write_versioned_tag(body, version)
    }
    write_versioned_tag(body, version)
  }
  body.write_bool(validate_only)
  write_versioned_tag(body, version)
  body.to_bytes()
}

///|
/// Decode an AlterClientQuotas v0-v1 response body: one result per
/// entry, echoing its entity.
pub fn decode_alter_client_quotas_response(
  version : Int,
  d : @buf.Decoder,
) -> (Array[AlterClientQuotasResult], Int) raise {
  let throttle = d.read_i32()
  let out : Array[AlterClientQuotasResult] = []
  let n = read_versioned_len(d, version)
  for _ in 0..<n {
    let error_code = d.read_i16()
    let error_message = read_versioned_nullable_string(d, version)
    let entity = decode_quota_entity(d, version)
    skip_versioned_tag(d, version)
    out.push({ error_code, error_message, entity, })
  }
  skip_versioned_tag(d, version)
  (out, throttle)
}

///|
pub async fn BrokerConnection::alter_client_quotas(
  self : BrokerConnection,
  entries : Array[ClientQuotaAlteration],
  validate_only? : Bool = false,
  timeout_ms? : Int = 30000,
) -> Array[AlterClientQuotasResult] {
  let version = self.api_version(API_ALTER_CLIENT_QUOTAS)
  let d = self.request_raw(
    API_ALTER_CLIENT_QUOTAS,
    version,
    encode_alter_client_quotas_request(entries, validate_only, version),
    timeout_ms~,
    flexible=version >= 1,
  )
  let (results, throttle) = decode_alter_client_quotas_response(version, d)
  self.note_throttle(throttle)
  results
}

///|
/// Encode a DescribeUserScramCredentials v0 request body: the users to
/// describe, or None to describe every user with credentials.
pub fn encode_describe_user_scram_request(users : Array[String]?) -> Bytes {
  let body = @buf.Encoder::new()
  match users {
    None => body.write_uvarint(0U)
    Some(users) => {
      body.write_compact_len(users.length())
      for name in users {
        body.write_compact_string(name)
        body.write_tag_buffer()
      }
    }
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DescribeUserScramCredentials v0 response body: one result
/// per user, each with its credential infos.
pub fn decode_describe_user_scram_response(
  d : @buf.Decoder,
) -> (DescribeUserScramResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let error_message = d.read_compact_nullable_string()
  let results : Array[DescribedScramCredentials] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let user = d.read_compact_string()
    let user_error = d.read_i16()
    let user_message = d.read_compact_nullable_string()
    let credential_infos : Array[ScramCredentialInfo] = []
    let count = d.read_compact_len()
    for _ in 0..<count {
      let mechanism = d.read_i8()
      let iterations = d.read_i32()
      d.skip_tag_buffer()
      credential_infos.push({ mechanism, iterations, })
    }
    d.skip_tag_buffer()
    results.push({
      user,
      error_code: user_error,
      error_message: user_message,
      credential_infos,
    })
  }
  d.skip_tag_buffer()
  ({ error_code, error_message, results, }, throttle)
}

///|
pub async fn BrokerConnection::describe_user_scram(
  self : BrokerConnection,
  users : Array[String]?,
  timeout_ms? : Int = 30000,
) -> DescribeUserScramResult {
  let d = self.request(
    API_DESCRIBE_USER_SCRAM_CREDENTIALS,
    self.api_version(API_DESCRIBE_USER_SCRAM_CREDENTIALS),
    encode_describe_user_scram_request(users),
    timeout_ms~,
  )
  let (result, throttle) = decode_describe_user_scram_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// Encode an AlterUserScramCredentials v0 request body: deletions and
/// upsertions, each followed by its own per-item result.
pub fn encode_alter_user_scram_request(
  deletions : Array[ScramCredentialDeletion],
  upsertions : Array[ScramCredentialUpsertion],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(deletions.length())
  for deletion in deletions {
    body.write_compact_string(deletion.name)
    body.write_i8(deletion.mechanism)
    body.write_tag_buffer()
  }
  body.write_compact_len(upsertions.length())
  for upsertion in upsertions {
    body.write_compact_string(upsertion.name)
    body.write_i8(upsertion.mechanism)
    body.write_i32(upsertion.iterations)
    write_compact_bytes(body, upsertion.salt)
    write_compact_bytes(body, upsertion.salted_password)
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode an AlterUserScramCredentials v0 response body: one result per
/// deletion and upsertion.
pub fn decode_alter_user_scram_response(
  d : @buf.Decoder,
) -> (Array[AlterUserScramResult], Int) raise {
  let throttle = d.read_i32()
  let out : Array[AlterUserScramResult] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let user = d.read_compact_string()
    let error_code = d.read_i16()
    let error_message = d.read_compact_nullable_string()
    d.skip_tag_buffer()
    out.push({ user, error_code, error_message, })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::alter_user_scram(
  self : BrokerConnection,
  deletions : Array[ScramCredentialDeletion],
  upsertions : Array[ScramCredentialUpsertion],
  timeout_ms? : Int = 30000,
) -> Array[AlterUserScramResult] {
  let d = self.request(
    API_ALTER_USER_SCRAM_CREDENTIALS,
    self.api_version(API_ALTER_USER_SCRAM_CREDENTIALS),
    encode_alter_user_scram_request(deletions, upsertions),
    timeout_ms~,
  )
  let (results, throttle) = decode_alter_user_scram_response(d)
  self.note_throttle(throttle)
  results
}

///|
/// List the quota entities matching the filter (an empty component list
/// matches everything). Retriable errors re-issue under the policy.
pub async fn Admin::describe_client_quotas(
  self : Admin,
  filter : ClientQuotaFilter,
) -> DescribeClientQuotasResult {
  self.with_retries(
    fn(result : DescribeClientQuotasResult) {
      admin_error_retriable(result.error_code)
    },
    async fn() {
      let conn = self.any_conn()
      conn.describe_client_quotas(filter, timeout_ms=self.request_timeout_ms)
    },
  )
}

///|
/// Alter the quota config of the given entities; one result per entry.
pub async fn Admin::alter_client_quotas(
  self : Admin,
  entries : Array[ClientQuotaAlteration],
  validate_only? : Bool = false,
) -> Array[AlterClientQuotasResult] {
  self.with_retries(
    fn(results : Array[AlterClientQuotasResult]) {
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
      conn.alter_client_quotas(
        entries,
        validate_only~,
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}

///|
/// Describe the SCRAM credentials of the given users (None = every user
/// with credentials); one result per user.
pub async fn Admin::describe_user_scram(
  self : Admin,
  users : Array[String]?,
) -> DescribeUserScramResult {
  self.with_retries(
    fn(result : DescribeUserScramResult) {
      let mut retry = admin_error_retriable(result.error_code)
      for user in result.results {
        if admin_error_retriable(user.error_code) {
          retry = true
        }
      }
      retry
    },
    async fn() {
      let conn = self.any_conn()
      conn.describe_user_scram(users, timeout_ms=self.request_timeout_ms)
    },
  )
}

///|
/// Delete and upsert SCRAM credentials; one result per affected user.
pub async fn Admin::alter_user_scram(
  self : Admin,
  deletions : Array[ScramCredentialDeletion],
  upsertions : Array[ScramCredentialUpsertion],
) -> Array[AlterUserScramResult] {
  self.with_retries(
    fn(results : Array[AlterUserScramResult]) {
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
      conn.alter_user_scram(
        deletions,
        upsertions,
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}
