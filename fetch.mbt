// Fetch v12-v16 and KIP-227 incremental fetch sessions.
//
// Field order is pinned from the Kafka 4.3 message schemas
// FetchRequest.json / FetchResponse.json. Version differences in range:
//   v12  flexible; topics by name; LastFetchedEpoch mandatory
//   v13  topics (and forgotten topics) by id (KIP-516)
//   v14  = v13 (new error codes only)
//   v15  ReplicaId dropped from the mandatory fields (KIP-903): the
//        follower state moved into a tagged field consumers omit
//   v16  = v15 (NodeEndpoints arrive as a tagged field, skipped)
//
// A fetch session lets the broker remember per-partition state: the
// first request is "full" (session id 0, epoch 0) and carries every
// partition; the broker replies with a session id, and later requests
// are incremental (epoch 1, 2, ...) carrying only partitions whose
// parameters changed, plus the partitions to forget. FETCH_SESSION_ID_
// NOT_FOUND / INVALID_FETCH_SESSION_EPOCH evict the session: rebuild
// from a full request.

///|
pub const FETCH_SESSION_ID_NOT_FOUND : Int = 70

///|
pub const INVALID_FETCH_SESSION_EPOCH : Int = 71

///|
/// Fetch epoch of a sessionless ("full") request.
pub const FETCH_FULL_EPOCH : Int = 0

///|
/// One partition to fetch, as the request carries it.
pub(all) struct FetchPartitionReq {
  index : Int
  leader_epoch : Int
  fetch_offset : Int64
  max_bytes : Int
} derive(@debug.Debug)

///|
/// One topic to fetch: both identifiers so any implemented version can
/// encode (v12 uses the name, v13+ the id).
pub(all) struct FetchTopicReq {
  name : String
  topic_id : Uuid
  partitions : Array[FetchPartitionReq]
} derive(@debug.Debug)

///|
/// One topic's partitions to remove from an established session.
pub(all) struct FetchTopicForgotten {
  name : String
  topic_id : Uuid
  partitions : Array[Int]
} derive(@debug.Debug)

///|
/// The session portion of a fetch request: what to put in the
/// session_id/session_epoch fields and which partitions to (re)send.
pub(all) struct FetchSessionReq {
  session_id : Int
  session_epoch : Int
  partitions : Array[FetchPartitionReq]
  forgotten : Array[FetchTopicForgotten]
} derive(@debug.Debug)

///|
/// Client-side state of one fetch session (one per leader connection).
/// `sent` mirrors what the broker last heard per partition, so the next
/// incremental request can carry only the changed ones.
pub struct FetchSession {
  mut session_id : Int
  mut next_epoch : Int
  mut sent : Map[Int, (Int64, Int)]
} derive(@debug.Debug)

///|
pub fn FetchSession::new() -> FetchSession {
  { session_id: 0, next_epoch: 1, sent: Map([]), }
}

///|
/// True while no session is established: requests go out as full fetches.
pub fn FetchSession::established(self : FetchSession) -> Bool {
  self.session_id != 0
}

///|
/// Build the next request against the desired partition parameters
/// (partition index -> (fetch offset, leader epoch)). Single-topic shape:
/// `topic_name`/`topic_id` identify the forgotten-partitions entries on
/// the wire (v12 needs the name, v13+ the id).
pub fn FetchSession::prepare(
  self : FetchSession,
  topic_name : String,
  topic_id : Uuid,
  wanted : Map[Int, (Int64, Int)],
  max_bytes : Int,
) -> FetchSessionReq {
  let mut full = !self.established()
  // Epochs are int32; a wrap is as good as eviction (Java does the same).
  if self.next_epoch <= FETCH_FULL_EPOCH || self.next_epoch > 0x7ffffffe {
    full = true
  }
  let parts : Array[FetchPartitionReq] = []
  let forgotten : Array[FetchTopicForgotten] = []
  if full {
    for index, param in wanted {
      parts.push({
        index,
        leader_epoch: param.1,
        fetch_offset: param.0,
        max_bytes,
      })
    }
    self.next_epoch = 1
    self.sent = Map([])
    for index, param in wanted {
      self.sent[index] = param
    }
    return {
      session_id: 0,
      session_epoch: FETCH_FULL_EPOCH,
      partitions: parts,
      forgotten,
    }
  }
  let epoch = self.next_epoch
  self.next_epoch = epoch + 1
  for index, param in wanted {
    match self.sent.get(index) {
      Some(prev) =>
        if prev != param {
          parts.push({
            index,
            leader_epoch: param.1,
            fetch_offset: param.0,
            max_bytes,
          })
        }
      None =>
        parts.push({
          index,
          leader_epoch: param.1,
          fetch_offset: param.0,
          max_bytes,
        })
    }
  }
  for index, _ in self.sent {
    if !wanted.contains(index) {
      forgotten.push({ name: topic_name, topic_id, partitions: [index], })
    }
  }
  self.sent = Map([])
  for index, param in wanted {
    self.sent[index] = param
  }
  {
    session_id: self.session_id,
    session_epoch: epoch,
    partitions: parts,
    forgotten,
  }
}

///|
/// Fold a response into the session state: adopt the broker's session id,
/// or restart with a full request after eviction (top-level error 70/71)
/// or when the broker closes the session (session id 0 in reply).
pub fn FetchSession::handle_response(
  self : FetchSession,
  response_session_id : Int,
  top_error_code : Int,
) -> Unit {
  if top_error_code == FETCH_SESSION_ID_NOT_FOUND ||
    top_error_code == INVALID_FETCH_SESSION_EPOCH {
    self.session_id = 0
    self.next_epoch = 1
    self.sent = Map([])
    return
  }
  if self.established() {
    if response_session_id == 0 {
      // The broker closed the session; the next request starts over.
      self.session_id = 0
      self.next_epoch = 1
      self.sent = Map([])
    }
  } else if response_session_id != 0 {
    self.session_id = response_session_id
  }
}

///|
/// Drop the session unconditionally (connection rebuilds, leadership
/// changes): the next request is a full fetch.
pub fn FetchSession::invalidate(self : FetchSession) -> Unit {
  self.session_id = 0
  self.next_epoch = 1
  self.sent = Map([])
}

///|
/// Encode a Fetch request body for v12-v16.
pub fn encode_fetch_request(
  version : Int,
  topics : Array[FetchTopicReq],
  session~ : FetchSessionReq,
  max_wait_ms~ : Int,
  min_bytes? : Int = 1,
  max_bytes? : Int = 0x7fffffff,
  isolation_level? : Int = 0,
  rack_id? : String = "",
) -> Bytes raise {
  if version < 12 || version > 16 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements Fetch v12-v16, got v\{version}",
    )
  }
  let body = @buf.Encoder::new()
  if version < 15 {
    body.write_i32(-1) // replica_id: consumer
  }
  // v15+ ReplicaState is a tagged field; consumers omit it (defaults to
  // replica -1), expressed by the empty top-level tag buffer below.
  body.write_i32(max_wait_ms)
  body.write_i32(min_bytes)
  body.write_i32(max_bytes)
  body.write_i8(isolation_level)
  body.write_i32(session.session_id)
  body.write_i32(session.session_epoch)
  body.write_compact_len(topics.length())
  for topic in topics {
    match version {
      12 => body.write_compact_string(topic.name)
      _ => body.write_bytes(topic.topic_id.to_bytes())
    }
    body.write_compact_len(topic.partitions.length())
    for p in topic.partitions {
      body.write_i32(p.index)
      body.write_i32(p.leader_epoch) // current_leader_epoch
      body.write_i64(p.fetch_offset)
      body.write_i32(-1) // last_fetched_epoch: no epoch tracking yet
      body.write_i64(-1L) // log_start_offset: follower-only
      body.write_i32(p.max_bytes)
      body.write_tag_buffer()
    }
    body.write_tag_buffer()
  }
  body.write_compact_len(session.forgotten.length())
  for topic in session.forgotten {
    match version {
      12 => body.write_compact_string(topic.name)
      _ => body.write_bytes(topic.topic_id.to_bytes())
    }
    body.write_compact_len(topic.partitions.length())
    for partition in topic.partitions {
      body.write_i32(partition)
    }
    body.write_tag_buffer()
  }
  body.write_compact_string(rack_id)
  body.write_tag_buffer()
  body.to_bytes()
}

///|
pub struct FetchPartitionResult {
  partition : Int
  error_code : Int
  high_watermark : Int64
  last_stable_offset : Int64
  log_start_offset : Int64
  records : Array[Record]
  /// The decoded batches with their metadata, so read_committed
  /// consumers can filter via collect_committed.
  batches : Array[DecodedBatch]
  /// The response's aborted-transactions list for this partition
  /// (isolation READ_COMMITTED fetches only).
  aborted_transactions : Array[AbortedTx]
  /// False when max_bytes truncated a trailing batch: `records` holds
  /// only complete batches, and the read position must not skip ahead
  /// past what was dropped.
  records_complete : Bool
}

///|
pub struct FetchTopicResult {
  name : String
  topic_id : Uuid
  partitions : Array[FetchPartitionResult]
}

///|
pub struct FetchResult {
  top_error_code : Int
  session_id : Int
  topics : Array[FetchTopicResult]
}

///|
/// Decode a Fetch v12-v16 response body. Tagged per-partition fields
/// (DivergingEpoch, CurrentLeader, SnapshotId) and the tagged top-level
/// NodeEndpoints are skipped. Returns the parsed result plus the throttle
/// hint in milliseconds.
pub fn decode_fetch_response(
  version : Int,
  d : @buf.Decoder,
) -> (FetchResult, Int) raise {
  if version < 12 || version > 16 {
    raise ProtocolError::ProtocolError(
      "moonkafka implements Fetch v12-v16, got v\{version}",
    )
  }
  let throttle = d.read_i32() // throttle_time_ms
  let top_error_code = d.read_i16()
  let session_id = d.read_i32()
  let topic_count = d.read_compact_len()
  let topics : Array[FetchTopicResult] = []
  for _ in 0..<topic_count {
    let (name, topic_id) = match version {
      12 => (d.read_compact_string(), Uuid::zero())
      _ => ("", Uuid::from_bytes(d.read_bytes(16)))
    }
    let partition_count = d.read_compact_len()
    let partitions : Array[FetchPartitionResult] = []
    for _ in 0..<partition_count {
      let partition = d.read_i32()
      let error_code = d.read_i16()
      let high_watermark = d.read_i64()
      let last_stable_offset = d.read_i64()
      let log_start_offset = d.read_i64()
      let aborted : Array[AbortedTx] = []
      let aborted_count = d.read_compact_len() // nullable array; -1 = null
      if aborted_count > 0 {
        for _ in 0..<aborted_count {
          let producer_id = d.read_i64()
          let first_offset = d.read_i64()
          d.skip_tag_buffer()
          aborted.push({ producer_id, first_offset, })
        }
      }
      d.skip(4) // preferred_read_replica
      let records_len = d.read_compact_len() // nullable records; -1 = null
      let (records, batches, truncated) = if records_len > 0 {
        let (detailed, cut) = decode_record_batches_detailed(
          d.read_bytes(records_len),
        )
        let flat : Array[Record] = []
        for batch in detailed {
          if batch.is_control {
            continue
          }
          for record in batch.records {
            flat.push(record)
          }
        }
        (flat, detailed, cut)
      } else {
        ([], [], false)
      }
      d.skip_tag_buffer()
      partitions.push({
        partition,
        error_code,
        high_watermark,
        last_stable_offset,
        log_start_offset,
        records,
        batches,
        aborted_transactions: aborted,
        records_complete: !truncated,
      })
    }
    d.skip_tag_buffer()
    topics.push({ name, topic_id, partitions, })
  }
  d.skip_tag_buffer()
  ({ top_error_code, session_id, topics, }, throttle)
}
