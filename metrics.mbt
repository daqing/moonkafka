// Producer metrics/counters (Phase 3): a simple counters struct, read on
// demand. Sent/failed/queued/in-flight live on the accumulator (updated
// under its lock by the sender task); throttle totals accumulate per
// broker connection.

///|
/// Snapshot of the producer's counters at read time.
pub struct ProducerMetrics {
  /// Records sitting in the accumulator's queues right now.
  records_queued : Int32
  /// Estimated wire bytes queued right now.
  bytes_queued : Int32
  /// Batches popped for sending and not yet resolved (on the wire or
  /// waiting for a retry round).
  batches_in_flight : Int32
  /// Records delivered and acknowledged by a broker.
  records_sent : Int32
  records_failed : Int32
  batches_sent : Int32
  batches_failed : Int32
  /// Cumulative throttle hints from Produce responses, summed over the
  /// cluster client's connections.
  throttle_time_ms : Int64
} derive(@debug.Debug)

///|
/// Read the current counters.
pub async fn Producer::metrics(self : Producer) -> ProducerMetrics {
  self.accumulator.metrics(self.cluster.total_throttle_ms())
}
