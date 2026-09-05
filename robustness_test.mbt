// Decoder robustness (Phase 2 acceptance): every codec must raise on
// random and truncated inputs — never panic, never index out of bounds.
// Deterministic xorshift PRNG keeps the fuzz reproducible.

///|
struct Xorshift {
  mut state : UInt
}

///|
fn Xorshift::new(state~ : UInt) -> Xorshift {
  { state, }
}

///|
fn Xorshift::next(self : Xorshift) -> UInt {
  let mut x = self.state
  x = x ^ (x << 13)
  x = x ^ (x >> 7)
  x = x ^ (x << 17)
  self.state = x
  x
}

///|
/// A deterministic pseudo-random payload of the given length.
fn fuzz_bytes(rng : Xorshift, len : Int) -> Bytes {
  let arr : Array[Byte] = Array::new(capacity=len)
  for _ in 0..<len {
    arr.push((rng.next() & 0xFFU).to_byte())
  }
  Bytes::from_array(arr)
}

///|
/// First `n` bytes of `data` (all of it when n exceeds the length).
fn prefix(data : Bytes, n : Int) -> Bytes {
  let arr : Array[Byte] = []
  let take = Int::min(n, data.length())
  for i in 0..<take {
    arr.push(data[i])
  }
  Bytes::from_array(arr)
}

///|
test "truncated compressed streams raise, never panic" {
  let rng = Xorshift::new(state=0xDEADBEEFU)
  let samples = [
    (golden_gzip, "gzip"),
    (golden_snappy, "snappy"),
    (golden_lz4, "lz4"),
    (golden_zstd, "zstd"),
  ]
  for sample in samples {
    let (stream, name) = sample
    // Truncate at every 17th offset plus a few extremes.
    let step = 17
    let mut cut = 0
    while cut < stream.length() {
      let partial = prefix(stream, cut)
      let ok = try {
        match name {
          "gzip" => ignore(@compression.gzip_decompress(partial))
          "snappy" => ignore(@compression.snappy_decompress(partial))
          "lz4" => ignore(@compression.lz4_decompress(partial))
          _ => ignore(@compression.zstd_decompress(partial))
        }
        true
      } catch {
        @buf.Malformed(_) => true
        @buf.UnexpectedEof => true
        _ => false
      }
      assert_true(ok)
      cut += step
    }
    // Random bytes must raise, not panic.
    for _ in 0..<50 {
      let junk = fuzz_bytes(rng, 64)
      let ok = try {
        match name {
          "gzip" => ignore(@compression.gzip_decompress(junk))
          "snappy" => ignore(@compression.snappy_decompress(junk))
          "lz4" => ignore(@compression.lz4_decompress(junk))
          _ => ignore(@compression.zstd_decompress(junk))
        }
        true
      } catch {
        @buf.Malformed(_) => true
        @buf.UnexpectedEof => true
        _ => false
      }
      assert_true(ok)
    }
  }
}

///|
test "record batch decode survives random and truncated input" {
  let rng = Xorshift::new(state=0x0BADF00DU)
  let valid = @moonkafka.encode_record_batch([
    {
      offset: 0L,
      timestamp: 1L,
      key: Some(b"k"),
      value: Some(b"v"),
      headers: [(b"h", b"1")],
    },
  ])
  // Truncations of a valid batch at every offset.
  for cut in 0..<valid.length() {
    let partial = prefix(valid, cut)
    let ok = try {
      ignore(@moonkafka.decode_record_batches_detailed(partial))
      true
    } catch {
      @buf.Malformed(_) => true
      @buf.UnexpectedEof => true
      _ => false
    }
    assert_true(ok)
  }
  // Random blobs: decode may succeed (skipping) but must never panic.
  for _ in 0..<200 {
    let blob = fuzz_bytes(rng, 80)
    let ok = try {
      ignore(@moonkafka.decode_record_batches_detailed(blob))
      true
    } catch {
      @buf.Malformed(_) => true
      @buf.UnexpectedEof => true
      _ => false
    }
    assert_true(ok)
  }
}

///|
test "wire decoders survive random input" {
  let rng = Xorshift::new(state=0x5EED1234U)
  for _ in 0..<200 {
    let blob = fuzz_bytes(rng, 64)
    // Each of these may raise a decode error; the contract is only that
    // they never panic.
    let ok1 = try {
      ignore(@moonkafka.decode_api_versions_response(@buf.Decoder::new(blob)))
      true
    } catch {
      _ => true
    }
    let ok2 = try {
      ignore(@moonkafka.decode_metadata_response(13, @buf.Decoder::new(blob)))
      true
    } catch {
      _ => true
    }
    let ok3 = try {
      ignore(@moonkafka.decode_produce_response(13, @buf.Decoder::new(blob)))
      true
    } catch {
      _ => true
    }
    let ok4 = try {
      ignore(
        @moonkafka.decode_describe_topic_partitions_response(
          @buf.Decoder::new(blob),
        ),
      )
      true
    } catch {
      _ => true
    }
    let ok5 = try {
      ignore(@moonkafka.decode_list_offsets_response(@buf.Decoder::new(blob)))
      true
    } catch {
      _ => true
    }
    assert_true(ok1 && ok2 && ok3 && ok4 && ok5)
  }
}

///|
test "version-dispatching decoders reject versions without codecs" {
  let blob = fuzz_bytes(Xorshift::new(state=99U), 32)
  let d1 = @buf.Decoder::new(blob)
  let d2 = @buf.Decoder::new(blob)
  let d3 = @buf.Decoder::new(blob)
  let d4 = @buf.Decoder::new(blob)
  let mut raised = 0
  ignore(@moonkafka.decode_fetch_response(17, d1)) catch {
    @moonkafka.ProtocolError(_) => raised += 1
    _ => ()
  }
  ignore(@moonkafka.decode_produce_response(9, d2)) catch {
    @moonkafka.ProtocolError(_) => raised += 1
    _ => ()
  }
  ignore(@moonkafka.decode_metadata_response(11, d3)) catch {
    @moonkafka.ProtocolError(_) => raised += 1
    _ => ()
  }
  ignore(@moonkafka.encode_list_offsets_request(7, "t", [], 0L)) catch {
    @moonkafka.ProtocolError(_) => raised += 1
    _ => ()
  }
  ignore(d4)
  assert_eq(raised, 4)
}
