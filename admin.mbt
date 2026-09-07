// The Admin client (Phase 5): a thin orchestration layer over the
// cluster client. Every op mirrors the protocol's result shape —
// per-topic/per-partition error codes travel as values in the returned
// structs, never as raises — and runs through the retry policy: results
// classified retriable by the op are re-issued with backoff up to
// `admin_retries` times. Transport failures raise immediately.

///|
/// Admin client settings on top of the shared transport config. The
/// retry knob is admin-specific: broker-side retriable errors (not
/// controller available, leader elections in flight, ...) re-issue the
/// whole call.
pub struct AdminConfig {
  common : CommonConfig
  /// How often a retriable admin result is re-issued.
  admin_retries : Int
} derive(@debug.Debug)

///|
pub fn AdminConfig::new(
  bootstrap_servers : Array[String],
  request_timeout_ms? : Int = 30000,
  security_protocol? : SecurityProtocol = Plaintext,
  sasl? : SaslConfig? = None,
  tls? : TlsClientOptions? = None,
  metadata_max_age_ms? : Int = 300000,
  admin_retries? : Int = 5,
) -> AdminConfig raise {
  if admin_retries < 0 {
    raise ProtocolError::ProtocolError(
      "admin_retries must be >= 0, got \{admin_retries}",
    )
  }
  {
    common: CommonConfig::new(
      bootstrap_servers,
      request_timeout_ms~,
      security_protocol~,
      sasl~,
      tls~,
      metadata_max_age_ms~,
    ),
    admin_retries,
  }
}

///|
pub struct Admin {
  request_timeout_ms : Int
  /// The retriable-result retry budget (the policy knob).
  admin_retries : Int
  retry_backoff_ms : Int
  retry_backoff_max_ms : Int
  priv cluster : ClusterClient
  mut closed : Bool
}

///|
/// Connect the admin client: one bootstrap negotiation, metadata cached
/// like the other clients. Ops run on any broker or on the controller
/// as each API requires.
pub async fn Admin::connect(
  group~ : @async.TaskGroup[Unit],
  host~ : String,
  port~ : Int,
) -> Admin {
  let config = AdminConfig::new(["\{host}:\{port}"])
  Admin::connect_with_config(group~, config)
}

///|
pub async fn Admin::connect_with_config(
  group~ : @async.TaskGroup[Unit],
  config : AdminConfig,
) -> Admin {
  // SASL credentials only apply on SASL security protocols.
  let sasl = match config.common.security_protocol {
    SaslPlaintext | SaslSsl => config.common.sasl
    _ => None
  }
  let use_tls = match config.common.security_protocol {
    Ssl | SaslSsl => true
    _ => false
  }
  let cluster = ClusterClient::connect(
    config.common.bootstrap_addresses(),
    client_id=config.common.client_id,
    request_timeout_ms=config.common.request_timeout_ms,
    metadata_max_age_ms=config.common.metadata_max_age_ms,
    sasl~,
    use_tls~,
    tls=config.common.tls,
  )
  {
    request_timeout_ms: config.common.request_timeout_ms,
    admin_retries: config.admin_retries,
    retry_backoff_ms: config.common.retry_backoff_ms,
    retry_backoff_max_ms: config.common.retry_backoff_max_ms,
    cluster,
    closed: false,
  }
}

///|
/// Close the admin client and tear its connections down.
pub fn Admin::close(self : Admin) -> Unit {
  if !self.closed {
    self.closed = true
    self.cluster.close()
  }
}

///|
/// A connection to any broker (the control connection): the default
/// route — most admin APIs forward to the controller broker-side.
async fn Admin::any_conn(self : Admin) -> BrokerConnection {
  self.cluster.control_conn()
}

///|
/// A connection to the cluster controller, for controller-routed APIs
/// (elect leaders, unregister broker, quorum reads).
async fn Admin::controller_conn(self : Admin) -> BrokerConnection {
  self.cluster.refresh_metadata(None)
  let controller = self.cluster.controller()
  self.cluster.connection(controller) catch {
    _ =>
      // Fall back to any broker: modern controllers accept forwarded
      // admin requests on any listener.
      self.any_conn()
  }
}

///|
/// Run one admin call under the retry policy: `retriable` inspects the
/// decoded result and asks for a re-issue; transport failures raise.
async fn[T] Admin::with_retries(
  self : Admin,
  retriable : (T) -> Bool,
  op : async () -> T,
) -> T {
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  for attempt in 0..<=self.admin_retries {
    let outcome = try? op()
    match outcome {
      Ok(result) => {
        if retriable(result) && attempt < self.admin_retries {
          @async.sleep(backoff.next_ms())
          continue
        }
        return result
      }
      Err(e) => raise e
    }
  }
  raise ProtocolError::ProtocolError("admin retries exhausted")
}

///|
/// True when the broker marks this error as retriable (the shared
/// classification drives the admin retry policy).
fn admin_error_retriable(code : Int) -> Bool {
  error_retriable(code)
}

///|
/// One topic's DescribeTopicPartitions result (error codes as values).
pub struct AdminTopicDescription {
  name : String
  error_code : Int
  topic_id : Uuid
  partitions : Array[PartitionInfo]
} derive(@debug.Debug)

///|
/// Inspect topic partitions: names, ids, leaders, replicas — the
/// paginated DescribeTopicPartitions v0 walkthrough (all pages
/// followed). Per-topic error codes come back as values; a retriable
/// topic error re-issues the whole call under the retry policy.
pub async fn Admin::describe_topic_partitions(
  self : Admin,
  topics : Array[String],
) -> Array[AdminTopicDescription] {
  self.with_retries(
    fn(results : Array[AdminTopicDescription]) {
      let mut retry = false
      for result in results {
        if admin_error_retriable(result.error_code) {
          retry = true
        }
      }
      retry
    },
    fn() { self.describe_topic_partitions_once(topics) },
  )
}

///|
async fn Admin::describe_topic_partitions_once(
  self : Admin,
  topics : Array[String],
) -> Array[AdminTopicDescription] {
  let conn = self.any_conn()
  // Walk the cursor to the end, merging pages per topic (pagination
  // splits one topic's partitions across pages).
  let by_name : Map[String, AdminTopicDescription] = Map([])
  let order : Array[String] = []
  let mut cursor : TopicPartitionCursor? = None
  for ;; {
    let page = conn.describe_topic_partitions(
      topics,
      response_partition_limit=100,
      cursor~,
      timeout_ms=self.request_timeout_ms,
    )
    for topic in page.topics {
      match by_name.get(topic.name) {
        Some(existing) =>
          for partition in topic.partitions {
            existing.partitions.push(partition)
          }
        None => {
          by_name[topic.name] = {
            name: topic.name,
            error_code: topic.error_code,
            topic_id: topic.topic_id,
            partitions: topic.partitions,
          }
          order.push(topic.name)
        }
      }
    }
    match page.next_cursor {
      Some(next) => cursor = Some(next)
      None => break
    }
  }
  order.map(fn(name) { by_name[name] })
}
