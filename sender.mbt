// The sender drain loop (Phase 3): wakes every few milliseconds, takes
// the accumulator's ready batches (linger expired, batch full, or the
// producer closing), groups them by partition leader, and puts one
// pipelined Produce request per leader on the wire — up to
// max_in_flight concurrent per connection, enforced by the connection's
// own semaphore. Recoverable failures requeue the affected batches at
// the front of their queues and retry with backoff; every batch must
// resolve within delivery_timeout_ms of its creation. acks=0 writes the
// requests without reading responses.

///|
/// Produce request outcome for one leader round.
priv enum SendRound {
  /// Every partition acknowledged (or written, for acks=0).
  RoundOk
  /// At least one failure worth retrying after recovery; batches were
  /// requeued at the fronts of their queues.
  RoundRetry
}

///|
/// The sender's tick when no batch is ready. Bounds the added latency of
/// linger_ms=0 producers; precise wakeup on first append lands with the
/// D6 background-task polish.
const SENDER_TICK_MS : Int = 5

///|
async fn Producer::sender_loop(self : Producer) -> Unit {
  let backoff = Backoff::new(
    base_ms=self.retry_backoff_ms,
    max_ms=self.retry_backoff_max_ms,
  )
  for ;; {
    if self.closed {
      // Final drain: force-close open batches so they all become ready,
      // empty the queues, resolve everything, then shut down.
      self.accumulator.force_close_all()
    }
    let ready = self.accumulator.drain_ready(
      @async.now(),
      self.delivery_timeout_ms,
    )
    if ready.is_empty() {
      if self.closed {
        break
      }
      @async.sleep(SENDER_TICK_MS)
      continue
    }
    // Group by partition leader from the cached metadata; batches whose
    // leader is unknown wait a round after a metadata refresh.
    let by_leader : Map[Int, Array[(Int, ProducerBatch)]] = Map([])
    let orphaned : Array[ProducerBatch] = []
    for batch in ready {
      match self.leader_of(batch.partition) {
        Some(leader) => {
          let group = by_leader.get_or_init(leader, fn() { [] })
          group.push((batch.partition, batch))
        }
        None => orphaned.push(batch)
      }
    }
    if !orphaned.is_empty() {
      for batch in orphaned {
        self.accumulator.requeue(batch)
      }
      let _ = self.cluster.refresh_metadata(Some([self.topic])) catch {
        _ => ()
      }
    }
    let mut failed = false
    for leader, entries in by_leader {
      match self.send_round(leader, entries) {
        RoundOk => ()
        RoundRetry => failed = true
      }
    }
    if failed && !self.closed {
      // Leadership changes and transport failures: re-dial the bootstrap
      // set, refresh metadata, and pace the next round with backoff.
      let _ = self.recover() catch { _ => () }
      @async.sleep(backoff.next_ms())
    }
  }
  self.cluster.close()
}

///|
/// The partition leader from the cached metadata, if known.
fn Producer::leader_of(self : Producer, partition : Int) -> Int? {
  match self.cluster.topic(self.topic) {
    Some(t) =>
      for p in t.partitions {
        if p.index == partition {
          return Some(p.leader)
        }
      }
    None => ()
  }
  None
}

///|
/// One Produce round for one leader: the round's batches (at most one
/// per partition) go out as a single request. Retriable failures requeue
/// their batch for the next round; terminal per-partition errors resolve
/// their batches' senders immediately.
async fn Producer::send_round(
  self : Producer,
  leader : Int,
  entries : Array[(Int, ProducerBatch)],
) -> SendRound {
  // The leader must be known from metadata; dial failures are retriable.
  guard self.cluster.broker(leader) is Some(_) else {
    for entry in entries {
      self.accumulator.requeue(entry.1)
    }
    return RoundRetry
  }
  // Resolve the batch -> wire bytes once per round.
  let partitions : Array[(Int, Bytes)] = []
  for entry in entries {
    entry.1.attempts += 1
    partitions.push((entry.0, entry.1.builder.to_bytes()))
  }
  if self.acks == 0 {
    // Fire-and-forget: write the frame, resolve everything with -1 (no
    // offset exists), never retry — the outcome is unknowable.
    let written = self.send_acks0(leader, partitions) catch { _ => false }
    for entry in entries {
      if written {
        self.accumulator.release(entry.1, BatchOk(-1L))
        self.batch_boundary(entry.1)
      } else {
        self.accumulator.release(
          entry.1,
          BatchError("produce request could not be written (acks=0)"),
        )
      }
    }
    return RoundOk
  }
  let conn = self.cluster.connection(leader) catch {
    _ => {
      for entry in entries {
        self.accumulator.requeue(entry.1)
      }
      return RoundRetry
    }
  }
  let results = conn.produce(
    self.topic,
    topic_id=self.topic_id,
    partitions~,
    acks=self.acks,
    timeout_ms=self.timeout_ms,
  ) catch {
    e =>
      match e {
        TransportError::ConnectionClosed(_) => {
          for entry in entries {
            self.accumulator.requeue(entry.1)
          }
          return RoundRetry
        }
        TransportError::RequestTimeout(_) => {
          for entry in entries {
            self.accumulator.requeue(entry.1)
          }
          return RoundRetry
        }
        // Anything else (SASL failure, protocol bug) fails the round's
        // senders loudly instead of spinning until delivery timeout.
        _ => {
          for entry in entries {
            self.accumulator.release(entry.1, BatchError("\{e}"))
          }
          return RoundOk
        }
      }
  }
  // Map results back per partition; entries the broker never answered
  // (retriable codes included) retry next round.
  let by_partition : Map[Int, ProducerBatch] = Map([])
  for entry in entries {
    by_partition[entry.0] = entry.1
  }
  let handled : Map[Int, Bool] = Map([])
  let mut retried = false
  for result in results {
    match by_partition.get(result.partition) {
      Some(batch) => {
        handled[result.partition] = true
        match result.error_code {
          0 => {
            self.accumulator.release(batch, BatchOk(result.base_offset))
            self.batch_boundary(batch)
          }
          code if error_retriable(code) => {
            // LEADER_NOT_AVAILABLE and friends: recoverable —
            // requeue, and the loop runs recovery after the round.
            self.accumulator.requeue(batch)
            retried = true
          }
          code =>
            self.accumulator.release(
              batch,
              BatchError(
                "Produce failed for partition \{result.partition}: \{error_name(code)}\{produce_error_detail(result)}",
              ),
            )
        }
      }
      None => ()
    }
  }
  for entry in entries {
    match handled.get(entry.0) {
      Some(_) => ()
      None => {
        self.accumulator.requeue(entry.1)
        retried = true
      }
    }
  }
  if retried {
    RoundRetry
  } else {
    RoundOk
  }
}

///|
/// acks=0 path: encode and write the Produce frame without registering
/// or awaiting a response.
async fn Producer::send_acks0(
  self : Producer,
  leader : Int,
  partitions : Array[(Int, Bytes)],
) -> Bool {
  let conn = self.cluster.connection(leader) catch { _ => return false }
  let version = conn.api_version(API_PRODUCE)
  let body = encode_produce_request(
    version,
    self.topic,
    self.topic_id,
    partitions,
    acks=0,
    timeout_ms=self.timeout_ms,
  )
  conn.send_only(API_PRODUCE, version, body)
  true
}

///|
/// The sticky partitioner's boundary for a batch the sender just sent:
/// rotation-closed batches fired at append time.
fn Producer::batch_boundary(self : Producer, batch : ProducerBatch) -> Unit {
  if !batch.boundary_fired {
    batch.boundary_fired = true
    self.router.on_batch_closed(self.partitions.length())
  }
}

///|
/// The broker's own explanation for a dropped batch: the summary message
/// and the offending record indices (KIP-467), when it reported any.
fn produce_error_detail(result : ProducePartitionResult) -> String {
  let detail = match result.error_message {
    Some(msg) => ": \{msg}"
    None => ""
  }
  if result.record_errors.is_empty() {
    return detail
  }
  let indices = result.record_errors
    .map(fn(err) { err.batch_index.to_string() })
    .join(",")
  "\{detail} (records [\{indices}])"
}
