// Kafka's murmur2 (org.apache.kafka.common.utils.Utils.murmur2), the hash
// behind the default producer partitioner. Java's unsigned right shifts (>>>)
// become reinterpret_as_uint shifts here.

///|
const MURMUR2_SEED : Int = 0x9747b28c

///|
const MURMUR2_M : Int = 0x5bd1e995

///|
/// Hash `data` exactly like Kafka's default partitioner. The result is a
/// signed 32-bit value; callers mask off the sign bit before taking modulo
/// partition count (see Producer::pick_partition).
pub fn murmur2(data : Bytes) -> Int {
  let length = data.length()
  let mut h = MURMUR2_SEED ^ length
  let length4 = length / 4
  for i in 0..<length4 {
    let i4 = i * 4
    let mut k = data[i4].to_int()
    k = k + (data[i4 + 1].to_int() << 8)
    k = k + (data[i4 + 2].to_int() << 16)
    k = k + (data[i4 + 3].to_int() << 24)
    k = k * MURMUR2_M
    k = k ^ logical_shr(k, 24)
    k = k * MURMUR2_M
    h = h * MURMUR2_M
    h = h ^ k
  }
  let rem = length % 4
  let base = length - rem
  if rem == 3 {
    h = h ^ (data[base + 2].to_int() << 16)
    h = h ^ (data[base + 1].to_int() << 8)
    h = h ^ data[base].to_int()
    h = h * MURMUR2_M
  } else if rem == 2 {
    h = h ^ (data[base + 1].to_int() << 8)
    h = h ^ data[base].to_int()
    h = h * MURMUR2_M
  } else if rem == 1 {
    h = h ^ data[base].to_int()
    h = h * MURMUR2_M
  }
  h = h ^ logical_shr(h, 13)
  h = h * MURMUR2_M
  h ^ logical_shr(h, 15)
}

///|
/// Java `>>>`: logical (unsigned) right shift for Int.
fn logical_shr(v : Int, shift : Int) -> Int {
  (v.reinterpret_as_uint() >> shift).reinterpret_as_int()
}
