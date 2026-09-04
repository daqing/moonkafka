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
pub struct Record {
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
/// A truncated trailing batch (possible when max_bytes splits a batch) is
/// silently dropped; the caller refetches from the last good offset + 1.
pub fn decode_record_batches(data : Bytes) -> Array[Record] raise DecodeError {
  let records : Array[Record] = []
  let mut pos = 0
  while pos + 12 <= data.length() {
    let batch_length = Decoder::new(data, start=pos + 8).read_i32() catch {
      _ => break
    }
    let batch_end = pos + 12 + batch_length
    if batch_length < RECORD_BATCH_HEADER_SIZE - 12 || batch_end > data.length() {
      break // truncated tail
    }
    decode_one_batch(data, pos, batch_end, records)
    pos = batch_end
  }
  records
}

///|
fn decode_one_batch(
  data : Bytes,
  start : Int,
  end : Int,
  out : Array[Record],
) -> Unit raise DecodeError {
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
  let stored_crc = Decoder::new(data, start=start + 17).read_i32() catch {
    _ => raise DecodeError::Malformed("short batch header")
  }
  let crc = crc32c(data, start=start + 21, end~)
  if crc != stored_crc.reinterpret_as_uint() {
    raise DecodeError::Malformed("record batch CRC32C mismatch")
  }
  let header = Decoder::new(data, start~)
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
fn decode_record(d : Decoder) -> Record raise DecodeError {
  let _length = d.read_varint()
  let _attributes = d.read_i8()
  let timestamp_delta = d.read_varlong()
  let offset_delta = d.read_varint()
  let key = read_nullable_varint_bytes(d)
  let value = read_nullable_varint_bytes(d)
  let header_count = d.read_varint()
  if header_count < 0 {
    raise DecodeError::Malformed("negative record header count")
  }
  for _ in 0..<header_count {
    let key_len = d.read_varint()
    if key_len < 0 {
      raise DecodeError::Malformed("null record header key")
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
fn read_nullable_varint_bytes(d : Decoder) -> Bytes? raise DecodeError {
  let len = d.read_varint()
  if len < -1 {
    raise DecodeError::Malformed("invalid negative bytes length")
  }
  if len == -1 {
    None
  } else {
    Some(d.read_bytes(len))
  }
}
