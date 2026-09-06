// Partition routing for the producer: strategy config (murmur2 for keyed
// records, sticky or round-robin for unkeyed), a sticky partition cache
// (KIP-794), and the manual per-send partition override.

///|
/// Partition assignment strategy, selected in `ProducerConfig`.
pub(all) enum Partitioner {
  /// Keyed records: murmur2(key) % n — Kafka-compatible placement. Unkeyed
  /// records: sticky batch partitioning, the Java client's default since
  /// KIP-794. This is the default strategy.
  Murmur2
  /// Keyed records: murmur2(key) % n. Unkeyed records: uniform round-robin
  /// (the pre-2.4 Java client behavior).
  Murmur2RoundRobin
  /// Every record: uniform round-robin, keys ignored (the Java
  /// RoundRobinPartitioner).
  RoundRobin
} derive(@debug.Debug, Eq)

///|
fn xorshift64(x : UInt64) -> UInt64 {
  let mut x = x
  x = x ^ (x << 13)
  x = x ^ (x >> 7)
  x = x ^ (x << 17)
  x
}

///|
/// Clock-seeded and mixed once. Producers created in the same millisecond
/// start on the same sticky partition, but boundary stepping still spreads
/// each producer's load — distinctness across producers is not a
/// correctness property.
fn next_sticky_seed() -> UInt64 {
  xorshift64(@async.now().reinterpret_as_uint64() ^ 0x9E3779B97F4A7C15UL)
}

///|
/// Sticky partition cache for unkeyed records (KIP-794): one partition is
/// reused until a batch boundary arrives, so consecutive small records
/// share a Produce request instead of each paying for its own partition.
/// Deviation from the Java StickyPartitionCache: a boundary steps to the
/// next partition rather than rolling random dice — it never repeats the
/// previous choice while n > 1 and stays deterministic; only the initial
/// pick is random (clock-seeded).
priv struct StickyPartitioner {
  mut current : Int // -1 = not chosen yet
  mut seed : UInt64
}

///|
fn StickyPartitioner::new(seed : UInt64) -> StickyPartitioner {
  { current: -1, seed, }
}

///|
/// The sticky partition out of n. The first call (or a partition-count
/// change after metadata refresh) seeds a choice; `on_new_batch` moves it.
fn StickyPartitioner::partition(self : StickyPartitioner, n : Int) -> Int {
  if n <= 1 {
    return 0
  }
  if self.current < 0 || self.current >= n {
    self.seed = xorshift64(self.seed)
    self.current = (self.seed >> 33).to_int() % n
  }
  self.current
}

///|
/// A batch boundary (batch full or linger expired): step to the next
/// partition, wrapping to 0.
fn StickyPartitioner::on_new_batch(self : StickyPartitioner, n : Int) -> Unit {
  if n > 1 {
    self.current = (self.partition(n) + 1) % n
  }
}

///|
/// Routing state: strategy, sticky cache, and the round-robin cursor,
/// kept apart from Producer so the selection rules are testable without a
/// connection.
priv struct PartitionRouter {
  strategy : Partitioner
  sticky : StickyPartitioner
  mut round_robin : Int
}

///|
fn PartitionRouter::new(
  strategy : Partitioner,
  seed : UInt64,
) -> PartitionRouter {
  { strategy, sticky: StickyPartitioner::new(seed), round_robin: 0, }
}

///|
/// Resolve the partition index for one record, in order of precedence:
/// 1. an explicit override wins, validated against the partition count;
/// 2. keyed records hash with murmur2 unless the strategy ignores keys;
/// 3. unkeyed records go sticky (default) or round-robin per the strategy.
fn PartitionRouter::pick(
  self : PartitionRouter,
  n : Int,
  key : Bytes?,
  override_partition : Int?,
) -> Int raise ProtocolError {
  match override_partition {
    Some(p) => {
      if p < 0 || p >= n {
        raise ProtocolError::ProtocolError(
          "partition \{p} is out of range (topic has \{n} partitions)",
        )
      }
      p
    }
    None =>
      match (self.strategy, key) {
        (RoundRobin, _) => self.next_round_robin(n)
        (_, Some(k)) => (@internal.murmur2(k) & 0x7fffffff) % n
        // Sticky: the same partition until a batch closes; the producer
        // calls on_batch_closed at batch boundaries.
        (Murmur2, None) => self.sticky.partition(n)
        (Murmur2RoundRobin, None) => self.next_round_robin(n)
      }
  }
}

///|
fn PartitionRouter::next_round_robin(self : PartitionRouter, n : Int) -> Int {
  if n <= 1 {
    return 0
  }
  let i = self.round_robin % n
  self.round_robin += 1
  i
}

///|
/// A batch boundary: the accumulator closed a batch (it filled, or its
/// flusher took it), so the next unkeyed batch under the Murmur2 strategy
/// moves to the next partition.
fn PartitionRouter::on_batch_closed(self : PartitionRouter, n : Int) -> Unit {
  self.sticky.on_new_batch(n)
}
