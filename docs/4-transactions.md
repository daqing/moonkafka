# Transactions: a cookbook

moonkafka supports the classic producer-transaction model over the
transaction-coordinator APIs. A producer configured with a `transactional_id`
gets idempotence + atomicity: batches are stamped with a (producerId, epoch,
baseSequence) and committed or aborted as a unit.

## Configuration

```moonbit
let producer = @moonkafka.Producer::connect(
  host=..., port=..., topic=...,
  transactional_id="txn-app-1",
  acks=-1,            // acks=all required for idempotence/transactions
  read_committed=true // consumer side; see below
)
```

## Lifecycle

`begin_transaction` → `send`/`send_all` → `commit_transaction` (or
`abort_transaction`). Offsets may be committed atomically inside the
transaction with the AddOffsetsToTxn + TxnOffsetCommit sequence.

```moonbit
producer.begin_transaction()
let offset = producer.send(key=..., value=...)
producer.commit_transaction()           // or producer.abort_transaction()
```

### What happens under the hood

1. **InitProducerId v5**: on first transactional use the client fetches a
   (producerId, producerEpoch).
2. **AddPartitionsToTxn v3**: partitions receiving records are added to the
   transaction with the transaction coordinator (FindCoordinator v4).
3. **Produce** batches are stamped with the epoch + per-partition base
   sequence.
4. **EndTxn v5**: `commit`/`abort` finalizes; the coordinator adopts the new
   epoch so that any later duplicate producerId+epoch is fenced.
5. **Transactional offset commits**: `TxnOffsetCommit` registers the group
   with the coordinator if it is not already registered.

### Error / retry semantics

- **Retriable**: transient coordinator errors in `FindCoordinator` /
  `AddPartitionsToTxn` / `EndTxn` retry with backoff and coordinator re-find.
- **Fencing**: `UNKNOWN_PRODUCER_ID` bumps the epoch; a stuck/down coordinator
  eventually forces an error rather than silently losing atomicity.
- **Commit policy**: a poisoned transaction (a batch was rejected) aborts;
  `abort_transaction` still lands even after a failed `commit_transaction`.

## Consumer side

To read only committed records use `read_committed` mode; the consumer filters
records belonging to aborted transactions and control (txn-marker) batches.

```moonbit
let consumer = @moonkafka.Consumer::connect(
  host=..., port=..., topic=...,
  read_committed=true,
)
```

## References

- KIP-98 (exactly-once transactional producer): <https://cwiki.apache.org/confluence/x/mgBJAw>
- KIP-890 (reshaped AddPartitionsToTxn), applied in the 4.3-era protocol the driver targets.