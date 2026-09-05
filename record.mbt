// Kafka RecordBatch v2 (magic = 2) decoding.
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
}

///|
const RECORD_BATCH_HEADER_SIZE : Int = 61

///|
const ATTR_COMPRESSION_MASK : Int = 0x07

///|
const ATTR_TIMESTAMP_TYPE : Int = 0x08

///|
const ATTR_CONTROL : Int = 0x20

///|
/// Decode all record batches concatenated in `data` (as found in a Fetch
/// response's records field). Compressed and control batches are skipped.
/// Returns the decoded records and whether the input had a truncated
/// trailing batch (possible when max_bytes splits a batch): the caller
/// must treat the position as the last complete record's offset + 1 and
/// refetch from there.
pub fn decode_record_batches_ex(
  data : Bytes,
) -> (Array[Record], Bool) raise @buf.DecodeError {
  let records : Array[Record] = []
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
    decode_one_batch(data, pos, batch_end, records)
    pos = batch_end
  }
  (records, truncated)
}

///|
/// Decode all record batches, ignoring any truncated trailing batch.
pub fn decode_record_batches(
  data : Bytes,
) -> Array[Record] raise @buf.DecodeError {
  decode_record_batches_ex(data).0
}

///|
fn decode_one_batch(
  data : Bytes,
  start : Int,
  end : Int,
  out : Array[Record],
) -> Unit raise @buf.DecodeError {
  let magic = {
    let v = data[start + 16].to_int()
    if v >= 128 {
      v - 256
    } else {
      v
    }
  }
  if magic != 2 {
    return
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
  header.skip(4) // lastOffsetDelta
  let base_timestamp = header.read_i64()
  let max_timestamp = header.read_i64()
  header.skip(8 + 2 + 4) // producerId, producerEpoch, baseSequence
  let record_count = header.read_i32()
  if (attributes & ATTR_COMPRESSION_MASK) != 0 {
    return // compressed batches not supported yet
  }
  if (attributes & ATTR_CONTROL) != 0 {
    return // control batches carry no user records
  }
  let log_append_time = (attributes & ATTR_TIMESTAMP_TYPE) != 0
  for _ in 0..<record_count {
    let record = decode_record(header)
    let timestamp = if log_append_time {
      max_timestamp
    } else {
      base_timestamp + record.timestamp
    }
    out.push({
      offset: base_offset + record.offset,
      timestamp,
      key: record.key,
      value: record.value,
    })
  }
}

///|
/// Decode one record: fields relative to the batch (offset/timestamp deltas).
/// All varints here are zig-zag encoded.
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
  for _ in 0..<header_count {
    let key_len = d.read_varint()
    if key_len < 0 {
      raise @buf.Malformed("null record header key")
    }
    d.skip(key_len)
    let value_len = d.read_varint()
    if value_len >= 0 {
      d.skip(value_len)
    }
  }
  {
    offset: Int64::from_int(offset_delta),
    timestamp: timestamp_delta,
    key,
    value,
  }
}

///|
/// Encode records into a single RecordBatch v2 (magic = 2), uncompressed,
/// base offset 0, create-time timestamps. Record timestamps are absolute ms
/// since epoch; offset deltas are assigned sequentially from 0.
pub fn encode_record_batch(records : Array[Record]) -> Bytes {
  let batch = @buf.Encoder::new()
  batch.write_i64(0L) // baseOffset
  batch.write_i32(0) // batchLength: patched below
  batch.write_i32(-1) // partitionLeaderEpoch: unknown to the producer
  batch.write_i8(2) // magic
  batch.write_i32(0) // crc: patched below
  batch.write_i16(0) // attributes: no compression, create-time
  batch.write_i32(records.length() - 1) // lastOffsetDelta (-1 when empty)
  let base_timestamp = match records.get(0) {
    Some(r) => r.timestamp
    None => 0L
  }
  let mut max_timestamp = base_timestamp
  for r in records {
    if r.timestamp > max_timestamp {
      max_timestamp = r.timestamp
    }
  }
  batch.write_i64(base_timestamp)
  batch.write_i64(max_timestamp)
  batch.write_i64(-1L) // producerId: none (no idempotence)
  batch.write_i16(-1) // producerEpoch
  batch.write_i32(-1) // baseSequence
  batch.write_i32(records.length())
  for i, record in records {
    let body = @buf.Encoder::new()
    body.write_i8(0) // attributes
    body.write_varlong(record.timestamp - base_timestamp)
    body.write_varint(i) // offsetDelta
    write_nullable_varint_bytes(body, record.key)
    write_nullable_varint_bytes(body, record.value)
    body.write_varint(0) // header count
    batch.write_varint(body.buf.length())
    batch.write_bytes(body.to_bytes())
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
