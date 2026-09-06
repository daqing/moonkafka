#!/usr/bin/env python3
"""Generate golden compression fixtures for moonkafka's codec tests.

The compressed streams are produced by INDEPENDENT implementations
(cramjam's Rust snappy/lz4/zstd, and Python's gzip), so the MoonBit
decoders are validated against foreign bytes, not their own encoder.
Also emits a spliced RecordBatch v2 per codec: the batch container comes
from the MoonBit encoder (uncompressed), then the records region is
replaced with the foreign-compressed blob and the CRC32C repaired —
exactly what a producer with that codec would put on the wire.

Run from the repo root:  python3 test/golden/generate.py
"""
import gzip as gzip_mod
import io
import os
import struct
import sys

import cramjam

HERE = os.path.dirname(os.path.abspath(__file__))
PAYLOAD = (b"moonkafka compression golden payload. "
           b"The quick brown fox jumps over the lazy dog 0123456789. " * 120)[:8192]


def xerial_wrap(data):
    """Kafka's snappy framing (snappy-java xerial): 0x82 'SNAPPY', int32
    version, int32 compat, then per-chunk int32 BE length + raw block."""
    out = io.BytesIO()
    out.write(b"\x82SNAPPY")
    out.write(struct.pack(">i", 1))
    out.write(struct.pack(">i", 1))
    for start in range(0, len(data), 32768):
        chunk = data[start:start + 32768]
        block = bytes(cramjam.snappy.compress_raw(chunk))
        out.write(struct.pack(">i", len(block)))
        out.write(block)
    return out.getvalue()


def lz4_frame_wrap_data(data):
    """LZ4 frame: magic, FLG (v1, block independent, no checksums),
    BD 64KB, HC=0, blocks with LE size prefixes, end mark. The MoonBit
    decoder does not validate HC, so a zero descriptor checksum is fine
    for a decode fixture."""
    out = io.BytesIO()
    out.write(struct.pack("<I", 0x184D2204))
    out.write(bytes([0x60, 0x40, 0x00]))  # FLG, BD, HC
    for start in range(0, len(data), 65536):
        chunk = data[start:start + 65536]
        # cramjam's compress_block prepends a 4-byte LE size: strip it,
        # the frame block header already carries the length.
        block = bytes(cramjam.lz4.compress_block(chunk))[4:]
        out.write(struct.pack("<I", len(block)))
        out.write(block)
    out.write(struct.pack("<I", 0))
    return out.getvalue()


def crc32c(data):
    poly = 0x82F63B78
    table = []
    for i in range(256):
        c = i
        for _ in range(8):
            c = (c >> 1) ^ poly if c & 1 else c >> 1
        table.append(c)
    crc = 0xFFFFFFFF
    for b in data:
        crc = table[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def records_region():
    """The uncompressed records region of the spliced batches: one record
    key="k" value="v" (matching the MoonBit encoder's layout)."""
    body = bytes([0x00,        # attributes i8
                  0x00])       # timestamp delta varlong 0
    body += b"\x00"           # offset delta varint 0
    body += b"\x02k"          # key: zig-zag varint length 1, "k"
    body += b"\x02v"          # value: zig-zag varint length 1, "v"
    body += b"\x00"           # header count
    return bytes([(len(body) << 1)]) + body


CODECS = {
    1: lambda region: gzip_mod.compress(region),
    2: lambda region: xerial_wrap(region),
    3: lambda region: lz4_frame_wrap_data(region),
    4: lambda region: bytes(cramjam.zstd.compress(region)),
}


def batch_bytes(region, attrs):
    """A minimal-but-complete RecordBatch v2 wrapper around a records
    region (mirrors the MoonBit encoder's layout: one record "ok")."""
    body = bytes([0x00, 0x02])           # attributes i8=0, tsDelta varlong=0
    body += b"\x00"                      # offsetDelta varint=0
    body += b"\x01k"                     # key: len 1, "k"
    body += b"\x01v"                     # value: len 1, "v"
    body += b"\x00"                      # header count 0
    record = bytes([len(body) << 1]) + body  # varint length prefix
    out = io.BytesIO()
    out.write(struct.pack(">q", 0))      # baseOffset
    out.write(struct.pack(">i", 0))      # batchLength (patched)
    out.write(struct.pack(">i", -1))     # partitionLeaderEpoch
    out.write(bytes([2]))                # magic
    out.write(struct.pack(">I", 0))      # crc (patched)
    out.write(struct.pack(">h", attrs))  # attributes
    out.write(struct.pack(">i", 0))      # lastOffsetDelta
    out.write(struct.pack(">q", 1000))   # baseTimestamp
    out.write(struct.pack(">q", 1000))   # maxTimestamp
    out.write(struct.pack(">q", -1))     # producerId
    out.write(struct.pack(">h", -1))     # producerEpoch
    out.write(struct.pack(">i", -1))     # baseSequence
    out.write(struct.pack(">i", 1))      # recordCount
    assert len(out.getvalue()) == 61
    out.write(region)
    data = bytearray(out.getvalue())
    struct.pack_into(">i", data, 8, len(data) - 12)
    crc = crc32c(bytes(data[21:]))
    struct.pack_into(">I", data, 17, crc)
    return bytes(data)


def emit(name, data):
    path = os.path.join(HERE, name + ".bin")
    with open(path, "wb") as f:
        f.write(data)
    print(f"{path} ({len(data)} bytes)")


def mbt_let(name, data):
    parts = ", ".join("b'\\x%02x'" % b for b in data)
    if len(parts) < 2000:
        return f"let {name} : Bytes = Bytes::from_array([{parts}])\n\n"
    # Wrap very long arrays: the parser dislikes megabyte-long lines.
    items = ["b'\\x%02x'" % b for b in data]
    lines = []
    for i in range(0, len(items), 16):
        lines.append("  " + ", ".join(items[i:i + 16]) + ",")
    return ("let %s : Bytes = Bytes::from_array([\n%s\n])\n\n" % (name, "\n".join(lines)))


def emit_mbt_all(path, pairs, batches):
    """One generated MoonBit test-data file: raw streams plus whole
    batches whose records region was replaced with the foreign-compressed
    blob (CRC repaired) — exactly what a codec-configured producer writes."""
    with open(path, "w") as f:
        f.write("// Generated by test/golden/generate.py — do not edit.\n")
        for name, data in pairs:
            f.write(mbt_let(name, data))
        for name, (codec_id, region) in batches.items():
            compressed = CODECS[codec_id](region)
            f.write(mbt_let(name, batch_bytes(compressed, codec_id)))
    print(path)


def main():
    with open(os.path.join(HERE, "payload.bin"), "wb") as f:
        f.write(PAYLOAD)

    gzip_stream = gzip_mod.compress(PAYLOAD)
    snappy_stream = xerial_wrap(PAYLOAD)
    lz4_stream = lz4_frame_wrap_data(PAYLOAD)
    zstd_stream = bytes(cramjam.zstd.compress(PAYLOAD))

    emit("golden_gzip", gzip_stream)
    emit("golden_snappy", snappy_stream)
    emit("golden_lz4", lz4_stream)
    emit("golden_zstd", zstd_stream)

    region = records_region()
    emit_mbt_all(
        os.path.join(HERE, "..", "..", "golden_test_data_test.mbt"),
        [
            ("golden_payload", PAYLOAD),
            ("golden_gzip", gzip_stream),
            ("golden_snappy", snappy_stream),
            ("golden_lz4", lz4_stream),
            ("golden_zstd", zstd_stream),
        ],
        {
            "golden_batch_gzip": (1, region),
            "golden_batch_snappy": (2, region),
            "golden_batch_lz4": (3, region),
            "golden_batch_zstd": (4, region),
        },
    )
    print("payload:", len(PAYLOAD), "bytes")


if __name__ == "__main__":
    main()
