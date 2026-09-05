/* Native stubs for the gzip (system zlib) and zstd (vendored
 * decompressor) codecs. All functions are single-shot and stateless:
 * MoonBit allocates the destination buffer and retries with a larger
 * one when the codec reports it was too small. */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#include "zstddeclib.c"

#include "moonbit.h"

/* gzip: inflate with auto-detection of gzip members (windowBits
 * 15 + 32). Returns bytes written, -2 when dest is too small, -1 on
 * any other failure. */
MOONBIT_FFI_EXPORT int32_t
moonkafka_gzip_decompress_into(
  uint8_t *src,
  int32_t src_len,
  uint8_t *dest,
  int32_t dest_cap
) {
  z_stream zs;
  memset(&zs, 0, sizeof(zs));
  if (inflateInit2(&zs, 15 + 32) != Z_OK) {
    return -1;
  }
  zs.next_in = src;
  zs.avail_in = (uInt)src_len;
  zs.next_out = dest;
  zs.avail_out = (uInt)dest_cap;
  int ret = inflate(&zs, Z_NO_FLUSH);
  inflateEnd(&zs);
  if (ret == Z_STREAM_END) {
    return (int32_t)(dest_cap - zs.avail_out);
  }
  if (ret == Z_OK && zs.avail_out == 0) {
    return -2;
  }
  return -1;
}

/* zlib's compressBound plus the gzip framing slack (header, ISIZE). */
MOONBIT_FFI_EXPORT int32_t
moonkafka_gzip_compress_bound(int32_t src_len) {
  return (int32_t)compressBound((uLong)src_len) + 64;
}

/* gzip: deflate with a gzip wrapper (windowBits 15 + 16), like every
 * Kafka producer. Return conventions as above. */
MOONBIT_FFI_EXPORT int32_t
moonkafka_gzip_compress_into(
  uint8_t *src,
  int32_t src_len,
  uint8_t *dest,
  int32_t dest_cap
) {
  z_stream zs;
  memset(&zs, 0, sizeof(zs));
  if (deflateInit2(&zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8,
                   Z_DEFAULT_STRATEGY) != Z_OK) {
    return -1;
  }
  zs.next_in = src;
  zs.avail_in = (uInt)src_len;
  zs.next_out = dest;
  zs.avail_out = (uInt)dest_cap;
  int ret = deflate(&zs, Z_FINISH);
  deflateEnd(&zs);
  if (ret == Z_STREAM_END) {
    return (int32_t)(dest_cap - zs.avail_out);
  }
  if (ret == Z_OK && zs.avail_out == 0) {
    return -2;
  }
  return -1;
}

/* zstd: the vendored frame decompressor. Whole-buffer decode; frames
 * without a known content size are rejected by the caller first. */
MOONBIT_FFI_EXPORT int32_t
moonkafka_zstd_decompress_into(
  uint8_t *src,
  int32_t src_len,
  uint8_t *dest,
  int32_t dest_cap
) {
  size_t ret = ZSTD_decompress(dest, (size_t)dest_cap, src, (size_t)src_len);
  if (ZSTD_isError(ret)) {
    return -1;
  }
  return (int32_t)ret;
}

/* Content size declared by the frame header: >= 0 known, -1 unknown
 * (ZSTD_CONTENTSIZE_UNKNOWN), -2 corrupt (ZSTD_CONTENTSIZE_ERROR). */
MOONBIT_FFI_EXPORT int64_t
moonkafka_zstd_frame_content_size(uint8_t *src, int32_t src_len) {
  unsigned long long v = ZSTD_getFrameContentSize(src, (size_t)src_len);
  if (v == ZSTD_CONTENTSIZE_ERROR) {
    return -2;
  }
  if (v == ZSTD_CONTENTSIZE_UNKNOWN) {
    return -1;
  }
  return (int64_t)v;
}
