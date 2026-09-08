# Version negotiating

moonkafka speaks the Kafka **4.3** wire protocol only. Every client will
Issue one `ApiVersions v3` handshake on its first connection to a broker and
pick, for each API request it sends, the highest version it implements that
the broker advertises (`max_version`), clipped to its own support. The table
below is the authoritative support matrix, generated from `versions.mbt`
(see `VersionRange::for_api_key`).

## Supported request versions (this driver)

| API                     | min | max | notes                                        |
|-------------------------|-----|-----|----------------------------------------------|
| Produce                 | 12  | 13  | v13 = topic-id addressing                    |
| Fetch                   | 12  | 16  | v12–v16, incremental fetch sessions (KIP-227)|
| Metadata                | 12  | 13  | topic-ids                                    |
| ListOffsets             | 10  | 11  | timestamp seeks, sentinels                   |
| FindCoordinator         | 4   | 4   | batched (KIP-699)                            |
| OffsetCommit            | 8   | 10  |                                              |
| OffsetFetch             | 8   | 10  |                                              |
| OffsetForLeaderEpoch    | 4   | 4   | truncation detection                         |
| CreateTopics            | 7   | 7   |                                              |
| DeleteTopics            | 6   | 6   |                                              |
| CreatePartitions        | 3   | 3   |                                              |
| DeleteRecords           | 2   | 2   |                                              |
| OffsetDelete            | 0   | 0   | (KIP-496 group offset deletion)              |
| DescribeTopicPartitions | 0   | 0   | paginated topic walking                      |
| DescribeCluster         | 0   | 2   |                                              |
| DescribeConfigs         | 4   | 4   |                                              |
| AlterConfigs            | 2   | 2   |                                              |
| IncrementalAlterConfigs | 1   | 1   | KIP-248                                     |
| ListConfigResources     | 0   | 1   |                                              |
| DescribeLogDirs         | 2   | 5   |                                              |
| ElectLeaders            | 2   | 2   |                                              |
| AlterPartitionReassignments | 0 | 1   |                                              |
| ListPartitionReassignments | 0 | 0   |                                              |
| UnregisterBroker        | 0   | 0   |                                              |
| DescribeQuorum          | 2   | 2   |                                              |
| UpdateFeatures          | 2   | 2   |                                              |
| JoinGroup               | 9   | 9   | classic consumer groups                      |
| SyncGroup               | 5   | 5   |                                              |
| Heartbeat               | 4   | 4   |                                              |
| LeaveGroup              | 5   | 5   |                                              |
| DescribeGroups          | 6   | 6   |                                              |
| ListGroups              | 5   | 5   |                                              |
| DeleteGroups            | 2   | 2   |                                              |
| ConsumerGroupDescribe   | 1   | 1   | KIP-848 consumer groups                      |
| ConsumerGroupHeartbeat  | 0   | 1   | KIP-848 (primary)                            |
| ListTransactions        | 1   | 2   |                                              |
| DescribeTransactions    | 0   | 0   |                                              |
| DescribeProducers       | 0   | 0   | KIP-52                                    |
| InitProducerId          | 5   | 5   | idempotence/transactions                     |
| AddPartitionsToTxn      | 3   | 3   |                                              |
| AddOffsetsToTxn         | 4   | 4   |                                              |
| EndTxn                  | 5   | 5   |                                              |
| TxnOffsetCommit         | 4   | 4   |                                              |
| DescribeAcls            | 2   | 3   |                                              |
| CreateAcls              | 2   | 3   |                                              |
| DeleteAcls              | 2   | 3   |                                              |
| DescribeClientQuotas    | 0   | 1   |                                              |
| AlterClientQuotas       | 0   | 1   |                                              |
| DescribeUserScramCredentials | 0 | 0 |                                        |
| AlterUserScramCredentials | 0  | 0   |                                              |
| ApiVersions             | 3   | 3   |                                              |
| SaslHandshake           | 1   | 1   |                                              |
| SaslAuthenticate        | 2   | 2   |                                              |
| ShareGroupHeartbeat     | 1   | 1   | KIP-932                                      |
| ShareFetch              | 1   | 2   | KIP-932                                      |
| ShareAcknowledge        | 1   | 2   | KIP-932                                      |
| DescribeShareGroupOffsets | 0 | 1  |                                              |
| AlterShareGroupOffsets  | 0   | 0   |                                              |
| DeleteShareGroupOffsets | 0   | 0   |                                              |
| GetTelemetrySubscriptions | 0 | 0   | KIP-714                                      |
| PushTelemetry           | 0   | 0   | KIP-714                                      |

## How negotiation works

1. On connect, send `ApiVersions v3` (flexible). Parse the advertised
   `min_version`/`max_version` per API key.
2. For each outbound request, choose `min(broker_max_version, driver_max)`.
   If `driver_max < broker_min_version` the driver does not support the
   broker's floor and the request is refused (or the API is skipped / the
   feature disabled) rather than silently talking an older protocol.
3. Version-sensitive topologies in the driver itself:

   - **Produce**: v13 when the broker advertises topic-id addressing and the
     topic has a known id; otherwise v12 (legacy topic-name addressing).
   - **Fetch**: the incremental-session (`KIP-227`) fields are only present
     from v12, so sessions are used only on v12+. Older (v ≤ 11) fetches are
     not offered at all because 4.x brokers start at v12.
   - **Consumer groups**: the KIP-848 new protocol is probed via
     `ApiVersions` advertising `ConsumerGroupHeartbeat`; a broker that does
     not advertise it triggers the `group_protocol` classic fallback.

See `docs/1-protocol.md` for the framing and compact-type details.