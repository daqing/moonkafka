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
pub const API_LIST_OFFSETS : Int = 2

///|
pub const API_METADATA : Int = 3

///|
pub const API_DESCRIBE_TOPIC_PARTITIONS : Int = 75

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
    API_FETCH => Some({ key: API_FETCH, name: "Fetch", min: 12, max: 12, })
    API_LIST_OFFSETS =>
      Some({ key: API_LIST_OFFSETS, name: "ListOffsets", min: 7, max: 7, })
    API_METADATA =>
      Some({ key: API_METADATA, name: "Metadata", min: 12, max: 13, })
    API_DESCRIBE_TOPIC_PARTITIONS =>
      Some({
        key: API_DESCRIBE_TOPIC_PARTITIONS,
        name: "DescribeTopicPartitions",
        min: 0,
        max: 0,
      })
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
