// CRC-32C (Castagnoli), reflected polynomial 0x82F63B78.
// Used to verify RecordBatch v2 checksums.

///|
let crc32c_table : FixedArray[UInt] = init_crc32c_table()

///|
fn init_crc32c_table() -> FixedArray[UInt] {
  let table = FixedArray::make(256, 0U)
  for i in 0..<256 {
    let mut crc = i.reinterpret_as_uint()
    for _ in 0..<8 {
      crc = if (crc & 1U) == 1U { (crc >> 1) ^ 0x82F63B78U } else { crc >> 1 }
    }
    table[i] = crc
  }
  table
}

///|
/// CRC-32C over data[start:end].
pub fn crc32c(data : Bytes, start? : Int, end? : Int) -> UInt {
  let start = start.unwrap_or(0)
  let end = end.unwrap_or(data.length())
  let mut crc = 0xFFFFFFFFU
  for i in start..<end {
    let idx = ((crc ^ data[i].to_int().reinterpret_as_uint()) & 0xFFU).reinterpret_as_int()
    crc = crc32c_table[idx] ^ (crc >> 8)
  }
  crc ^ 0xFFFFFFFFU
}
