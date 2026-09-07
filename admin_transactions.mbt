// Admin transaction/producer introspection (Phase 5): DescribeProducers v0,
// DescribeTransactions v0, and ListTransactions v2. These are the KIP-890
// admin-side calls: who is actively producing into each partition, and
// which transactions are in flight. Field order is pinned from the
// corresponding *Request/Response.json in the Kafka 4.3 tree under
// clients/src/main/resources/common/message/. All three are flexible from
// v0. Partition leaders answer DescribeProducers and the transaction
// coordinator answers DescribeTransactions, so the broker forwards both and
// they run on the any-broker control connection (like DeleteRecords).
// ListTransactions, like ListGroups, is answered from each broker's own
// coordinator state and therefore fans out to every broker and merges.

///|
/// One active producer for a partition, as DescribeProducers reports it.
pub struct AdminActiveProducer {
  producer_id : Int64
  producer_epoch : Int
  /// The last sequence number the producer sent, or -1.
  last_sequence : Int
  /// The last timestamp the producer sent (wall-clock ms), or -1.
  last_timestamp : Int64
  /// The epoch of the producer's transaction coordinator.
  coordinator_epoch : Int
  /// The offset the producer's current transaction started at, or -1 when
  /// it is not in a transaction.
  current_txn_start_offset : Int64
} derive(@debug.Debug)

///|
/// One partition's DescribeProducers result (error codes as values).
pub struct AdminDescribeProducerPartition {
  partition_index : Int
  error_code : Int
  error_message : String?
  active_producers : Array[AdminActiveProducer]
} derive(@debug.Debug)

///|
/// One topic's DescribeProducers result.
pub struct AdminDescribeProducersTopic {
  name : String
  partitions : Array[AdminDescribeProducerPartition]
} derive(@debug.Debug)

///|
/// Encode a DescribeProducers v0 request body: the topics and the
/// partition indexes to list producers for.
pub fn encode_describe_producers_request(
  topics : Array[(String, Array[Int])],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(topics.length())
  for topic in topics {
    let (name, partitions) = topic
    body.write_compact_string(name)
    body.write_compact_len(partitions.length())
    for partition in partitions {
      body.write_i32(partition)
    }
    body.write_tag_buffer()
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DescribeProducers v0 response body: one topic per requested
/// topic, one partition per requested partition index, with the active
/// producers and per-partition error codes as values.
pub fn decode_describe_producers_response(
  d : @buf.Decoder,
) -> (Array[AdminDescribeProducersTopic], Int) raise {
  let throttle = d.read_i32()
  let out : Array[AdminDescribeProducersTopic] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let name = d.read_compact_string()
    let partitions : Array[AdminDescribeProducerPartition] = []
    let pcount = d.read_compact_len()
    for _ in 0..<pcount {
      let partition_index = d.read_i32()
      let error_code = d.read_i16()
      let error_message = d.read_compact_nullable_string()
      let producers : Array[AdminActiveProducer] = []
      let acount = d.read_compact_len()
      for _ in 0..<acount {
        let producer_id = d.read_i64()
        let producer_epoch = d.read_i32()
        let last_sequence = d.read_i32()
        let last_timestamp = d.read_i64()
        let coordinator_epoch = d.read_i32()
        let current_txn_start_offset = d.read_i64()
        d.skip_tag_buffer()
        producers.push({
          producer_id,
          producer_epoch,
          last_sequence,
          last_timestamp,
          coordinator_epoch,
          current_txn_start_offset,
        })
      }
      d.skip_tag_buffer()
      partitions.push({
        partition_index,
        error_code,
        error_message,
        active_producers: producers,
      })
    }
    d.skip_tag_buffer()
    out.push({ name, partitions, })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::describe_producers(
  self : BrokerConnection,
  topics : Array[(String, Array[Int])],
  timeout_ms? : Int = 30000,
) -> Array[AdminDescribeProducersTopic] {
  let d = self.request(
    API_DESCRIBE_PRODUCERS,
    self.api_version(API_DESCRIBE_PRODUCERS),
    encode_describe_producers_request(topics),
    timeout_ms~,
  )
  let (result, throttle) = decode_describe_producers_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// Describe the active producers of the given topic partitions: each
/// partition reports the producers currently holding it with their id,
/// epoch, last sequence/timestamp, and the offset their current
/// transaction started at. Per-partition error codes travel as values and
/// a retriable one re-issues the whole call under the retry policy.
/// Partition leaders answer, so the call runs on any broker.
pub async fn Admin::describe_producers(
  self : Admin,
  topics : Array[(String, Array[Int])],
) -> Array[AdminDescribeProducersTopic] {
  self.with_retries(
    fn(results : Array[AdminDescribeProducersTopic]) {
      let mut retry = false
      for topic in results {
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
      conn.describe_producers(topics, timeout_ms=self.request_timeout_ms)
    },
  )
}

///|
/// One topic and its partitions inside an in-flight transaction, as
/// DescribeTransactions reports it.
pub struct AdminTransactionTopic {
  topic : String
  partitions : Array[Int]
} derive(@debug.Debug)

///|
/// One transactional id's DescribeTransactions result (error codes as
/// values). `topics` is empty once the transaction is no longer active.
pub struct AdminDescribeTransaction {
  error_code : Int
  transactional_id : String
  /// The state name: Empty, Ongoing, PrepareCommit, PrepareAbort,
  /// CompleteCommit, CompleteAbort, or Dead.
  transaction_state : String
  transaction_timeout_ms : Int
  transaction_start_time_ms : Int64
  producer_id : Int64
  producer_epoch : Int
  topics : Array[AdminTransactionTopic]
} derive(@debug.Debug)

///|
/// Encode a DescribeTransactions v0 request body: the transactional ids to
/// describe. An empty array asks for nothing (the broker returns nothing).
pub fn encode_describe_transactions_request(
  transactional_ids : Array[String],
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(transactional_ids.length())
  for id in transactional_ids {
    body.write_compact_string(id)
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a DescribeTransactions v0 response body: one transaction state
/// per requested transactional id, with its error code, state, timeout,
/// start time, producer id/epoch, and the topic-partitions it currently
/// spans.
pub fn decode_describe_transactions_response(
  d : @buf.Decoder,
) -> (Array[AdminDescribeTransaction], Int) raise {
  let throttle = d.read_i32()
  let out : Array[AdminDescribeTransaction] = []
  let n = d.read_compact_len()
  for _ in 0..<n {
    let error_code = d.read_i16()
    let transactional_id = d.read_compact_string()
    let transaction_state = d.read_compact_string()
    let transaction_timeout_ms = d.read_i32()
    let transaction_start_time_ms = d.read_i64()
    let producer_id = d.read_i64()
    let producer_epoch = d.read_i16()
    let topics : Array[AdminTransactionTopic] = []
    let tcount = d.read_compact_len()
    for _ in 0..<tcount {
      let topic = d.read_compact_string()
      let partitions : Array[Int] = []
      let pcount = d.read_compact_len()
      for _ in 0..<pcount {
        partitions.push(d.read_i32())
      }
      d.skip_tag_buffer() // TopicData
      topics.push({ topic, partitions, })
    }
    d.skip_tag_buffer() // TransactionState
    out.push({
      error_code,
      transactional_id,
      transaction_state,
      transaction_timeout_ms,
      transaction_start_time_ms,
      producer_id,
      producer_epoch,
      topics,
    })
  }
  d.skip_tag_buffer()
  (out, throttle)
}

///|
pub async fn BrokerConnection::describe_transactions(
  self : BrokerConnection,
  transactional_ids : Array[String],
  timeout_ms? : Int = 30000,
) -> Array[AdminDescribeTransaction] {
  let d = self.request(
    API_DESCRIBE_TRANSACTIONS,
    self.api_version(API_DESCRIBE_TRANSACTIONS),
    encode_describe_transactions_request(transactional_ids),
    timeout_ms~,
  )
  let (result, throttle) = decode_describe_transactions_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// Describe the given transactional ids: their current state, timeout,
/// start time, producer id/epoch, and the topic-partitions the
/// transaction currently spans. Per-id error codes travel as values and a
/// retriable one re-issues the whole call under the retry policy. The
/// transaction coordinator answers, so the broker forwards and the call
/// runs on any broker.
pub async fn Admin::describe_transactions(
  self : Admin,
  transactional_ids : Array[String],
) -> Array[AdminDescribeTransaction] {
  self.with_retries(
    fn(results : Array[AdminDescribeTransaction]) {
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
      conn.describe_transactions(
        transactional_ids,
        timeout_ms=self.request_timeout_ms,
      )
    },
  )
}

///|
/// One transactional id listed by ListTransactions.
pub struct AdminListedTransaction {
  transactional_id : String
  producer_id : Int64
  /// The state name: Empty, Ongoing, PrepareCommit, PrepareAbort,
  /// CompleteCommit, CompleteAbort, or Dead.
  transaction_state : String
} derive(@debug.Debug)

///|
/// One ListTransactions result. The error is per-call: a single top-level
/// error covers the whole listing, and `transactions` is empty when it is
/// non-zero. `unknown_state_filters` echoes any request filters the
/// coordinator did not recognize.
pub struct AdminListTransactionsResult {
  error_code : Int
  unknown_state_filters : Array[String]
  transactions : Array[AdminListedTransaction]
} derive(@debug.Debug)

///|
/// Encode a ListTransactions request body. The request grows by version:
/// v1 adds the duration filter, v2 adds the nullable transactional-id
/// pattern, so `api_version` decides whether that trailing field is
/// written.
pub fn encode_list_transactions_request(
  api_version : Int,
  state_filters : Array[String],
  producer_id_filters : Array[Int64],
  duration_filter : Int64,
  transactional_id_pattern : String?,
) -> Bytes {
  let body = @buf.Encoder::new()
  body.write_compact_len(state_filters.length())
  for state in state_filters {
    body.write_compact_string(state)
  }
  body.write_compact_len(producer_id_filters.length())
  for producer_id in producer_id_filters {
    body.write_i64(producer_id)
  }
  body.write_i64(duration_filter)
  if api_version >= 2 {
    body.write_compact_nullable_string(transactional_id_pattern)
  }
  body.write_tag_buffer()
  body.to_bytes()
}

///|
/// Decode a ListTransactions response body: a top-level error, the
/// unknown state filters, and the transactions this broker coordinates.
pub fn decode_list_transactions_response(
  d : @buf.Decoder,
) -> (AdminListTransactionsResult, Int) raise {
  let throttle = d.read_i32()
  let error_code = d.read_i16()
  let unknown : Array[String] = []
  let ucount = d.read_compact_len()
  for _ in 0..<ucount {
    unknown.push(d.read_compact_string())
  }
  let transactions : Array[AdminListedTransaction] = []
  let tcount = d.read_compact_len()
  for _ in 0..<tcount {
    let transactional_id = d.read_compact_string()
    let producer_id = d.read_i64()
    let transaction_state = d.read_compact_string()
    d.skip_tag_buffer() // TransactionState
    transactions.push({ transactional_id, producer_id, transaction_state, })
  }
  d.skip_tag_buffer()
  ({ error_code, unknown_state_filters: unknown, transactions, }, throttle)
}

///|
pub async fn BrokerConnection::list_transactions(
  self : BrokerConnection,
  state_filters : Array[String],
  producer_id_filters : Array[Int64],
  duration_filter : Int64,
  transactional_id_pattern : String?,
  timeout_ms? : Int = 30000,
) -> AdminListTransactionsResult {
  let version = self.api_version(API_LIST_TRANSACTIONS)
  let d = self.request(
    API_LIST_TRANSACTIONS,
    version,
    encode_list_transactions_request(
      version, state_filters, producer_id_filters, duration_filter, transactional_id_pattern,
    ),
    timeout_ms~,
  )
  let (result, throttle) = decode_list_transactions_response(d)
  self.note_throttle(throttle)
  result
}

///|
/// List the transactions a cluster's brokers coordinate, optionally
/// filtered by state, producer id, minimum duration, and transactional-id
/// pattern (empty/None filters mean "all"). Like ListGroups, the broker
/// answers only from its own coordinator state, so the call fans out to
/// every broker in the cached metadata snapshot and merges the results,
/// de-duplicating transactions seen on multiple brokers and unioning the
/// unknown-filter echoes. The returned error is the first non-zero error
/// any broker reported; retriable ones re-issue the whole call under the
/// retry policy. Transport failures raise.
pub async fn Admin::list_transactions(
  self : Admin,
  state_filters? : Array[String] = [],
  producer_id_filters? : Array[Int64] = [],
  duration_filter? : Int64 = -1L,
  transactional_id_pattern? : String? = None,
) -> AdminListTransactionsResult {
  self.with_retries(
    fn(result : AdminListTransactionsResult) {
      admin_error_retriable(result.error_code)
    },
    async fn() {
      self.list_transactions_once(
        state_filters, producer_id_filters, duration_filter, transactional_id_pattern,
      )
    },
  )
}

///|
async fn Admin::list_transactions_once(
  self : Admin,
  state_filters : Array[String],
  producer_id_filters : Array[Int64],
  duration_filter : Int64,
  transactional_id_pattern : String?,
) -> AdminListTransactionsResult {
  self.cluster.refresh_metadata(None)
  let ids = self.cluster.broker_ids()
  let by_id : Map[String, AdminListedTransaction] = Map([])
  let order : Array[String] = []
  let unknown : Array[String] = []
  let mut error_code = 0
  for node in ids {
    let conn = self.cluster.connection(node)
    let result = conn.list_transactions(
      state_filters,
      producer_id_filters,
      duration_filter,
      transactional_id_pattern,
      timeout_ms=self.request_timeout_ms,
    )
    if result.error_code != 0 && error_code == 0 {
      error_code = result.error_code
    }
    for filter in result.unknown_state_filters {
      if !unknown.contains(filter) {
        unknown.push(filter)
      }
    }
    for txn in result.transactions {
      if by_id.contains(txn.transactional_id) {
        continue
      }
      by_id[txn.transactional_id] = txn
      order.push(txn.transactional_id)
    }
  }
  {
    error_code,
    unknown_state_filters: unknown,
    transactions: order.map(fn(id) { by_id[id] }),
  }
}
