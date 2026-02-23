#pragma once

// ============================================================
// compress.hpp -- zstd compression wrapper
// ============================================================

#include "platform.hpp"
#include <vector>
#include <string>
#include <stdexcept>

// zstd header from bundled library
#include "zstd.h"

namespace compress {

// Compression level 1 = fastest
static constexpr int ZSTD_LEVEL = 1;

// Returns the maximum compressed size for a given input size
inline size_t max_compressed_size(size_t input_size) {
    return ZSTD_compressBound(input_size);
}

// Compress src -> dst (dst must be pre-sized to at least max_compressed_size(src_len))
// Returns actual compressed size, or 0 if compression made it larger
inline size_t compress(void* dst, size_t dst_cap, const void* src, size_t src_len) {
    size_t result = ZSTD_compress(dst, dst_cap, src, src_len, ZSTD_LEVEL);
    if (ZSTD_isError(result)) {
        throw std::runtime_error(std::string("ZSTD compress error: ") + ZSTD_getErrorName(result));
    }
    return result;
}

// Decompress src -> dst (dst_cap must be original size)
// Returns actual decompressed size
inline size_t decompress(void* dst, size_t dst_cap, const void* src, size_t src_len) {
    size_t result = ZSTD_decompress(dst, dst_cap, src, src_len);
    if (ZSTD_isError(result)) {
        throw std::runtime_error(std::string("ZSTD decompress error: ") + ZSTD_getErrorName(result));
    }
    return result;
}

// Compress to a resizable buffer; returns compressed data
inline std::vector<u8> compress_to_vec(const void* src, size_t src_len) {
    size_t cap = max_compressed_size(src_len);
    std::vector<u8> buf(cap);
    size_t sz = compress(buf.data(), cap, src, src_len);
    buf.resize(sz);
    return buf;
}

// Decompress to a buffer of known original size
inline std::vector<u8> decompress_to_vec(const void* src, size_t src_len, size_t original_size) {
    std::vector<u8> buf(original_size);
    size_t sz = decompress(buf.data(), original_size, src, src_len);
    buf.resize(sz);
    return buf;
}

// Extension whitelist: should we compress this file?
// Returns true if the file extension is compressible
inline bool should_compress(const std::string& path) {
    // Do NOT compress already-compressed formats
    static const char* const no_compress[] = {
        ".gz", ".bz2", ".xz", ".zst", ".lz4", ".br",
        ".zip", ".7z", ".rar", ".tar",
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".heic",
        ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm",
        ".mp3", ".aac", ".ogg", ".flac", ".opus", ".m4a",
        ".pdf",
        nullptr
    };

    // Find extension
    auto dot_pos = path.rfind('.');
    if (dot_pos == std::string::npos) return true;

    std::string ext = path.substr(dot_pos);
    // Lowercase
    for (auto& c : ext) {
        if (c >= 'A' && c <= 'Z') c = (char)(c + 32);
    }

    for (int i = 0; no_compress[i]; ++i) {
        if (ext == no_compress[i]) return false;
    }
    return true;
}

} // namespace compress
