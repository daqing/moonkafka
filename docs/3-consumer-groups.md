# Consumer groups

There are two membership protocols; a `Consumer` picks one via its
`group_protocol` config. See `config.mbt` (`ConsumerGroupProtocol`) and the
KIP-848 vs. classic comparison below.

## Selecting the protocol

`group_protocol` accepts `New (KIP-848)`, `Classic`, or an ordered `Fallback`
list. The default is `New` with classic fallback. On connect the client probes
the broker's `ApiVersions` response:

- If it advertises `ConsumerGroupHeartbeat`, the new (KIP-848) protocol can
  be used.
- A broker that does not (or a config that pins `Classic`) uses the classic
  `JoinGroup/SyncGroup/Heartbeat` path.

`ConsumerGroupHeartbeat` makes the broker assign partitions and drive the
rebalance; the client only reports its subscription and applies the assignment.
The classic path runs the Java-compatible assignors client-side.

## KIP-848 (new) path — `Consumer::subscribe`

- `SubscribeGroup` / `ConsumerGroupHeartbeat v0..1` membership with
  **client-generated member ids** and server-driven assignment applied
  atomically around the `RebalanceListener` hooks.
- Static membership (stable member id across sessions), regex subscription,
  no-op heartbeat to keep the session alive, graceful leave.
- The assignment surfaces through `Consumer::assignment()`; introspection
  helpers `position`, `committed`, `assignment`, `group_metadata`,
  `member_identity`, `membership_error` describe the live membership.

## Classic (compat) path — `Consumer::subscribe_classic`

- `JoinGroup/SyncGroup/Heartbeat` with range, round-robin, sticky, and
  cooperative-sticky assignors (two-round incremental rebalancing).
- Static membership, graceful leave, rebalance listener hooks.

> **feature parity**: regex subscription works differently in the two paths —
> KIP-848 does it server-side, classic does it client-side via a metadata
> scan. They are documented as behaving differently rather than force-unified.

## Consumer surface (shared)

`subscribe`/`assign` (manual) split, `seek` family, `poll`, per-partition
`pause`/`resume`, `max_poll_records` / `max_partition_fetch_bytes` caps,
`auto_offset_reset` policies, leader-epoch truncation detection, committed
offset tracking with sync/async commit and autocommit, `read_committed`
filtering, and `max_poll_interval_ms` enforcement.

## Share groups (KIP-932)

`ShareConsumer` is a separate surface over `ShareGroupHeartbeat`,
`ShareFetch v2` acquisition with delivery-count caps, and `ShareAcknowledge`
(accept/release/reject/renew). See `share_consumer.mbt`.

## References

- KIP-848: <https://cwiki.apache.org/confluence/x/5HU0BQ>
- KIP-932: <https://cwiki.apache.org/confluence/x/gAvTBw>
- Classic assignors: range / round-robin / sticky / cooperative-sticky