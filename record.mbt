// Kafka RecordBatch v2 (magic = 2) decoding and encoding.
//
// Batch header layout (offsets from batch start, all big-endian):
//   0   baseOffset            INT64
//   8   batchLength           INT32  (bytes after this field)
//   12  partitionLeaderEpoch  INT32
//   16  magic                 INT8   (= 2)
//   17  crc                   UINT32 (CRC32C over bytes [21, 12+batchLength))
//   21  attributes            INT16  (bits 0-2 compression, 3 timestampType,
//                                     4 transactional, 5 control batch)
//   23  lastOffsetDelta       INT32
//   27  baseTimestamp         INT64
//   35  maxTimestamp          INT64
//   43  producerId            INT64
//   51  producerEpoch         INT16
//   53  baseSequence          INT32
//   57  recordCount           INT32
//   61  records...

///|
pub(all) struct Record {
  offset : Int64
  timestamp : Int64
  key : Bytes?
  value : Bytes?
  headers : Array[(Bytes, Bytes)]
} derive(@debug.Debug)

///|
const RECORD_BATCH_HEADER_SIZE : Int = 61

///|
const ATTR_COMPRESSION_MASK : Int = 0x07

///|
const ATTR_TIMESTAMP_TYPE : Int = 0x08

///|
const ATTR_TRANSACTIONAL : Int = 0x10

///|
const ATTR_CONTROL : Int = 0x20

///|
/// One decoded record batch with the metadata read_committed filtering
/// and the idempotence machinery need: identity, flags, and whether the
/// byte stream ended mid-batch (max_bytes split).
pub(all) struct DecodedBatch {
  records : Array[Record]
  /// Absolute offset of the batch's last record (base + lastOffsetDelta);
  /// the fetcher advances read positions past whole batches with it.
  last_offset : Int64
  producer_id : Int64
  producer_epoch : Int
  base_sequence : Int
  is_control : Bool
  is_transactional : Bool
  truncated : Bool
} derive(@debug.Debug)

///|
/// One entry of a Fetch response's aborted-transactions list.
pub(all) struct AbortedTx {
  producer_id : Int64
  first_offset : Int64
} derive(@debug.Debug)

///|
/// Decode all record batches concatenated in `data` (as found in a Fetch
/// response's records field) with their batch-level metadata. Compressed
/// batches are skipped until their codec lands; control batches decode
/// as empty records with is_control set. `truncated` marks a batch the
/// input cut short (possible when max_bytes splits a batch): it is not
/// included in the result, so the caller's read position must not skip
/// past it.
pub fn decode_record_batches_detailed(
  data : Bytes,
) -> (Array[DecodedBatch], Bool) raise @buf.DecodeError {
  let batches : Array[DecodedBatch] = []
  let mut pos = 0
  let mut truncated = false
  while pos < data.length() {
    if pos + 12 > data.length() {
      truncated = true
      break
    }
    let batch_length = @buf.Decoder::new(data, start=pos + 8).read_i32() catch {
      _ => {
        truncated = true
        break
      }
    }
    let batch_end = pos + 12 + batch_length
    if batch_length < RECORD_BATCH_HEADER_SIZE - 12 || batch_end > data.length() {
      truncated = true
      break // truncated tail
    }
    batches.push(decode_one_batch(data, pos, batch_end))
    pos = batch_end
  }
  (batches, truncated)
}

///|
/// Decode all record batches into flat record lists, reporting whether a
/// truncated trailing batch was dropped (batches remain offset-safe).
pub fn decode_record_batches_ex(
  data : Bytes,
) -> (Array[Record], Bool) raise @buf.DecodeError {
  let (batches, truncated) = decode_record_batches_detailed(data)
  let records : Array[Record] = []
  for batch in batches {
    // Control batches carry transaction markers, not user data: they
    // stay out of the flat view (read_committed consumers filter them
    // via collect_committed on the detailed view instead).
    if batch.is_control {
      continue
    }
    for record in batch.records {
      records.push(record)
    }
  }
  (records, truncated)
}

///|
/// The record key as UTF-8, when present.
pub fn Record::key_utf8(self : Record) -> String? {
  self.key.map(fn(bytes) { @utf8.decode_lossy(bytes[:]) })
}

///|
/// The record value as UTF-8, when present (tombstones decode to None).
pub fn Record::value_utf8(self : Record) -> String? {
  self.value.map(fn(bytes) { @utf8.decode_lossy(bytes[:]) })
}

///|
/// Decode all record batches, ignoring any truncated trailing batch.
pub fn decode_record_batches(
  data : Bytes,
) -> Array[Record] raise @buf.DecodeError {
  decode_record_batches_ex(data).0
}

///|
/// Read-committed primitive: flatten batches to records, dropping
/// control batches (they carry markers, not data) and every record of an
/// aborted transaction — a transactional batch is aborted when its
/// producer id appears in `aborted` and the record offset reaches that
/// entry's first_offset. The Fetch response's aborted-tx list feeds
/// `aborted`; the caller bookkeeps producerId/firstOffset across polls.
pub fn collect_committed(
  batches : Array[DecodedBatch],
  aborted : Array[AbortedTx],
) -> Array[Record] {
  let out : Array[Record] = []
  for batch in batches {
    if batch.is_control {
      continue
    }
    for record in batch.records {
      if batch.is_transactional &&
        is_aborted(batch.producer_id, record.offset, aborted) {
        continue
      }
      out.push(record)
    }
  }
  out
}

///|
fn is_aborted(
  producer_id : Int64,
  offset : Int64,
  aborted : Array[AbortedTx],
) -> Bool {
  for entry in aborted {
    if entry.producer_id == producer_id && offset >= entry.first_offset {
      return true
    }
  }
  false
}

///|
fn decode_one_batch(
  data : Bytes,
  start : Int,
  end : Int,
) -> DecodedBatch raise @buf.DecodeError {
  let magic = {
    let v = data[start + 16].to_int()
    if v >= 128 {
      v - 256
    } else {
      v
    }
  }
  if magic != 2 {
    return { // not a v2 batch: skip like the old formats' bytes
      records: [],
      last_offset: -1L,
      producer_id: -1L,
      producer_epoch: -1,
      base_sequence: -1,
      is_control: false,
      is_transactional: false,
      truncated: false,
    }
  }
  let stored_crc = @buf.Decoder::new(data, start=start + 17).read_i32() catch {
    _ => raise @buf.Malformed("short batch header")
  }
  let crc = @internal.crc32c(data, start=start + 21, end~)
  if crc != stored_crc.reinterpret_as_uint() {
    raise @buf.Malformed("record batch CRC32C mismatch")
  }
  let header = @buf.Decoder::new(data, start~)
  let base_offset = header.read_i64()
  header.skip(4 + 4 + 1 + 4) // batchLength, leaderEpoch, magic, crc
  let attributes = header.read_i16()
  let last_offset_delta = header.read_i32()
  let base_timestamp = header.read_i64()
  let max_timestamp = header.read_i64()
  let producer_id = header.read_i64()
  let producer_epoch = header.read_i16()
  let base_sequence = header.read_i32()
  let record_count = header.read_i32()
  let is_control = (attributes & ATTR_CONTROL) != 0
  let is_transactional = (attributes & ATTR_TRANSACTIONAL) != 0
  let records : Array[Record] = []
  let compression_id = attributes & ATTR_COMPRESSION_MASK
  let log_append_time = (attributes & ATTR_TIMESTAMP_TYPE) != 0
  // Compressed batches carry the records region as one whole-batch
  // compressed blob right after the fixed header; decompress, then parse
  // the same record_count records out of it. Reserved attribute bits
  // (above 4) skip the batch like the Java client does.
  let records_decoder : @buf.Decoder = if compression_id != 0 {
    let codec = match @compression.codec_by_id(compression_id) {
      Some(codec) => codec
      None =>
        return {
          records,
          last_offset: base_offset + last_offset_delta.to_int64(),
          producer_id,
          producer_epoch,
          base_sequence,
          is_control,
          is_transactional,
          truncated: false,
        }
    }
    let blob = header.read_bytes(header.remaining())
    @buf.Decoder::new((codec.decompress)(blob))
  } else {
    header
  }
  for _ in 0..<record_count {
    let record = decode_record(records_decoder)
    let timestamp = if log_append_time {
      max_timestamp
    } else {
      base_timestamp + record.timestamp
    }
    records.push({
      offset: base_offset + record.offset,
      timestamp,
      key: record.key,
      value: record.value,
      headers: record.headers,
    })
  }
  {
    records,
    last_offset: base_offset + last_offset_delta.to_int64(),
    producer_id,
    producer_epoch,
    base_sequence,
    is_control,
    is_transactional,
    truncated: false,
  }
}

///|
/// Decode one record: fields relative to the batch (offset/timestamp deltas)
/// plus the header array. All varints here are zig-zag encoded.
fn decode_record(d : @buf.Decoder) -> Record raise @buf.DecodeError {
  let _length = d.read_varint()
  let _attributes = d.read_i8()
  let timestamp_delta = d.read_varlong()
  let offset_delta = d.read_varint()
  let key = read_nullable_varint_bytes(d)
  let value = read_nullable_varint_bytes(d)
  let header_count = d.read_varint()
  if header_count < 0 {
    raise @buf.Malformed("negative record header count")
  }
  let headers : Array[(Bytes, Bytes)] = []
  for _ in 0..<header_count {
    let key_len = d.read_varint()
    if key_len < 0 {
      raise @buf.Malformed("null record header key")
    }
    let header_key = d.read_bytes(key_len)
    let header_value = read_nullable_varint_bytes(d)
    headers.push((header_key, header_value.unwrap_or(b"")))
  }
  {
    offset: Int64::from_int(offset_delta),
    timestamp: timestamp_delta,
    key,
    value,
    headers,
  }
}

///|
/// Encode records into a single RecordBatch v2 (magic = 2), uncompressed,
/// create-time timestamps. Record offsets are rebased to sequential
/// deltas from `base_offset`; timestamps are absolute ms since epoch.
pub fn encode_record_batch_with_base_offset(
  base_offset : Int64,
  records : Array[Record],
) -> Bytes {
  let builder = RecordBatchBuilder::new(base_offset~)
  for record in records {
    builder.append(
      record.timestamp,
      key=record.key,
      value=record.value,
      headers=record.headers,
    )
  }
  builder.to_bytes()
}

///|
/// Encode records into a single uncompressed batch at base offset 0.
pub fn encode_record_batch(records : Array[Record]) -> Bytes {
  encode_record_batch_with_base_offset(0L, records)
}

///|
/// Incremental batch builder for the producer accumulator: appends
/// records, tracks the encoded size, and finalizes with the CRC.
pub struct RecordBatchBuilder {
  base_offset : Int64
  bodies : Array[Bytes]
  /// Sum of the encoded record bodies plus their length varints.
  mut body_bytes : Int
  mut base_timestamp : Int64
  mut max_timestamp : Int64
  mut last_offset : Int64
  /// Idempotence identity stamped by the producer; -1/-1/-1 = none.
  mut producer_id : Int64
  mut producer_epoch : Int
  mut base_sequence : Int
  /// Marks the batch transactional (attributes bit 4); read_committed
  /// consumers filter it against the aborted-transactions list.
  mut transactional : Bool
} derive(@debug.Debug)

///|
pub fn RecordBatchBuilder::new(base_offset? : Int64 = 0L) -> RecordBatchBuilder {
  {
    base_offset,
    bodies: [],
    body_bytes: 0,
    base_timestamp: -1L,
    max_timestamp: -1L,
    last_offset: -1L,
    producer_id: -1L,
    producer_epoch: -1,
    base_sequence: -1,
    transactional: false,
  }
}

///|
/// Stamp the idempotence identity: the batch's records get sequence
/// numbers base_sequence..base_sequence+count-1 on the wire. Re-stamping
/// (epoch bump after UNKNOWN_PRODUCER_ID) regenerates them at to_bytes.
pub fn RecordBatchBuilder::set_idempotence(
  self : RecordBatchBuilder,
  producer_id : Int64,
  producer_epoch : Int,
  base_sequence : Int,
) -> Unit {
  self.producer_id = producer_id
  self.producer_epoch = producer_epoch
  self.base_sequence = base_sequence
}

///|
/// Mark the batch transactional (attributes bit 4).
pub fn RecordBatchBuilder::set_transactional(self : RecordBatchBuilder) -> Unit {
  self.transactional = true
}

///|
/// Append one record. `timestamp` is absolute ms since epoch; offsets
/// are assigned sequentially from the builder's base offset.
pub fn RecordBatchBuilder::append(
  self : RecordBatchBuilder,
  timestamp : Int64,
  key? : Bytes? = None,
  value? : Bytes? = None,
  headers? : Array[(Bytes, Bytes)] = [],
) -> Unit {
  let encoded = self.encode_record_body(timestamp, key, value, headers)
  if self.base_timestamp < 0L {
    // The first record fixes the batch's base timestamp; later deltas
    // are relative to it and must be non-negative (monotonic stamps,
    // as the broker requires).
    self.base_timestamp = timestamp
  }
  self.body_bytes += encoded.length() + varint_len(encoded.length())
  self.bodies.push(encoded)
  if timestamp > self.max_timestamp {
    self.max_timestamp = timestamp
  }
  self.last_offset = self.last_offset + 1L
}

///|
/// The encoded record body `append` would store, without mutating the
/// builder. The base timestamp for the first record is treated as the
/// record's own timestamp, so the encoded deltas match what append will
/// write.
fn RecordBatchBuilder::encode_record_body(
  self : RecordBatchBuilder,
  timestamp : Int64,
  key : Bytes?,
  value : Bytes?,
  headers : Array[(Bytes, Bytes)],
) -> Bytes {
  let base = if self.base_timestamp < 0L {
    timestamp
  } else {
    self.base_timestamp
  }
  let body = @buf.Encoder::new()
  body.write_i8(0) // attributes
  body.write_varlong(timestamp - base)
  body.write_varint((self.last_offset + 1L).to_int())
  write_nullable_varint_bytes(body, key)
  write_nullable_varint_bytes(body, value)
  body.write_varint(headers.length())
  for entry in headers {
    let (header_key, header_value) = entry
    body.write_varint(header_key.length())
    body.write_bytes(header_key)
    write_nullable_varint_bytes(body, Some(header_value))
  }
  body.to_bytes()
}

///|
/// Encoded size that `append` would add — the record body plus its
/// length varint — without mutating the builder. The accumulator's
/// has-room check for batch_size and buffer_memory accounting.
pub fn RecordBatchBuilder::estimated_append_size(
  self : RecordBatchBuilder,
  timestamp : Int64,
  key? : Bytes? = None,
  value? : Bytes? = None,
  headers? : Array[(Bytes, Bytes)] = [],
) -> Int {
  let encoded = self.encode_record_body(timestamp, key, value, headers)
  encoded.length() + varint_len(encoded.length())
}

///|
/// Number of records appended so far.
pub fn RecordBatchBuilder::count(self : RecordBatchBuilder) -> Int {
  self.bodies.length()
}

///|
/// Upper bound on the finalized batch size: fixed header plus record
/// bodies (header fields other than bodies do not grow with records).
pub fn RecordBatchBuilder::estimated_size(self : RecordBatchBuilder) -> Int {
  RECORD_BATCH_HEADER_SIZE + self.body_bytes
}

///|
/// Highest assigned absolute offset; base_offset - 1 when empty.
pub fn RecordBatchBuilder::last_assigned_offset(
  self : RecordBatchBuilder,
) -> Int64 {
  self.base_offset + self.last_offset
}

///|
/// Finalize the batch: header with rebased timestamps, CRC32C patched in.
pub fn RecordBatchBuilder::to_bytes(self : RecordBatchBuilder) -> Bytes {
  let count = self.bodies.length()
  let base_timestamp = if self.base_timestamp < 0L {
    0L
  } else {
    self.base_timestamp
  }
  let max_timestamp = if self.max_timestamp < base_timestamp {
    base_timestamp
  } else {
    self.max_timestamp
  }
  let batch = @buf.Encoder::new()
  batch.write_i64(self.base_offset) // baseOffset
  batch.write_i32(0) // batchLength: patched below
  batch.write_i32(-1) // partitionLeaderEpoch: unknown to the producer
  batch.write_i8(2) // magic
  batch.write_i32(0) // crc: patched below
  batch.write_i16(if self.transactional { ATTR_TRANSACTIONAL } else { 0 })
  batch.write_i32(count - 1) // lastOffsetDelta (-1 when empty)
  batch.write_i64(base_timestamp)
  batch.write_i64(max_timestamp)
  batch.write_i64(self.producer_id) // producerId
  batch.write_i16(self.producer_epoch) // producerEpoch
  batch.write_i32(self.base_sequence) // baseSequence
  batch.write_i32(count)
  for body in self.bodies {
    batch.write_varint(body.length()) // record length prefix
    batch.write_bytes(body)
  }
  // Patch batchLength and the CRC32C over the bytes after the crc field.
  let batch_length = batch.buf.length() - 12
  for i in 0..<4 {
    batch.buf[8 + i] = (batch_length >> (24 - 8 * i)).to_byte()
  }
  let crc = @internal.crc32c(batch.to_bytes(), start=21)
  for i in 0..<4 {
    batch.buf[17 + i] = ((crc >> (24 - 8 * i)) & 0xFFU).to_byte()
  }
  batch.to_bytes()
}

///|
/// Byte length of a zig-zag varint encoding of n (small sizes only).
fn varint_len(n : Int) -> Int {
  let mut v = ((n << 1) ^ (n >> 31)).reinterpret_as_uint()
  let mut len = 1
  while v >= 0x80U {
    v = v >> 7
    len += 1
  }
  len
}

///|
fn write_nullable_varint_bytes(e : @buf.Encoder, b : Bytes?) -> Unit {
  match b {
    None => e.write_varint(-1)
    Some(b) => {
      e.write_varint(b.length())
      e.write_bytes(b)
    }
  }
}

///|
fn read_nullable_varint_bytes(
  d : @buf.Decoder,
) -> Bytes? raise @buf.DecodeError {
  let len = d.read_varint()
  if len < -1 {
    raise @buf.Malformed("invalid negative bytes length")
  }
  if len == -1 {
    None
  } else {
    Some(d.read_bytes(len))
  }
}
