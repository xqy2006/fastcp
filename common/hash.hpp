#pragma once

// ============================================================
// hash.hpp -- xxHash3 wrappers for FastCP
// ============================================================

#include "platform.hpp"
#include <cstddef>
#include <array>
#include <stdexcept>

// xxhash.h is included via the xxhash static library
// We include it here with XXH_STATIC_LINKING_ONLY for XXH3 API
#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

namespace hash {

// 16-byte (128-bit) hash result
using Hash128 = std::array<u8, 16>;

// Compute xxh3_128 of a memory buffer
inline Hash128 xxh3_128(const void* data, size_t len) {
    XXH128_hash_t h = XXH3_128bits(data, len);
    Hash128 result;
    // Store as big-endian for determinism
    u64 lo = h.low64;
    u64 hi = h.high64;
    result[ 0] = (u8)(hi >> 56);
    result[ 1] = (u8)(hi >> 48);
    result[ 2] = (u8)(hi >> 40);
    result[ 3] = (u8)(hi >> 32);
    result[ 4] = (u8)(hi >> 24);
    result[ 5] = (u8)(hi >> 16);
    result[ 6] = (u8)(hi >>  8);
    result[ 7] = (u8)(hi      );
    result[ 8] = (u8)(lo >> 56);
    result[ 9] = (u8)(lo >> 48);
    result[10] = (u8)(lo >> 40);
    result[11] = (u8)(lo >> 32);
    result[12] = (u8)(lo >> 24);
    result[13] = (u8)(lo >> 16);
    result[14] = (u8)(lo >>  8);
    result[15] = (u8)(lo      );
    return result;
}

// Compute xxh3_32 (lower 32 bits of xxh3_64) of a memory buffer
inline u32 xxh3_32(const void* data, size_t len) {
    return (u32)(XXH3_64bits(data, len) & 0xFFFFFFFFull);
}

// Streaming hasher for xxh3_128
class StreamHasher128 {
public:
    StreamHasher128() {
        state_ = XXH3_createState();
        if (!state_) throw std::runtime_error("XXH3_createState failed");
        reset();
    }

    ~StreamHasher128() {
        if (state_) XXH3_freeState(state_);
    }

    StreamHasher128(const StreamHasher128&) = delete;
    StreamHasher128& operator=(const StreamHasher128&) = delete;

    void reset() {
        XXH3_128bits_reset(state_);
    }

    void update(const void* data, size_t len) {
        XXH3_128bits_update(state_, data, len);
    }

    Hash128 digest() const {
        XXH128_hash_t h = XXH3_128bits_digest(state_);
        Hash128 result;
        u64 lo = h.low64;
        u64 hi = h.high64;
        result[ 0] = (u8)(hi >> 56); result[ 1] = (u8)(hi >> 48);
        result[ 2] = (u8)(hi >> 40); result[ 3] = (u8)(hi >> 32);
        result[ 4] = (u8)(hi >> 24); result[ 5] = (u8)(hi >> 16);
        result[ 6] = (u8)(hi >>  8); result[ 7] = (u8)(hi      );
        result[ 8] = (u8)(lo >> 56); result[ 9] = (u8)(lo >> 48);
        result[10] = (u8)(lo >> 40); result[11] = (u8)(lo >> 32);
        result[12] = (u8)(lo >> 24); result[13] = (u8)(lo >> 16);
        result[14] = (u8)(lo >>  8); result[15] = (u8)(lo      );
        return result;
    }

private:
    XXH3_state_t* state_;
};

// Streaming hasher for xxh3_64 (used for per-chunk 32-bit hash)
class StreamHasher64 {
public:
    StreamHasher64() {
        state_ = XXH3_createState();
        if (!state_) throw std::runtime_error("XXH3_createState failed");
        reset();
    }

    ~StreamHasher64() {
        if (state_) XXH3_freeState(state_);
    }

    StreamHasher64(const StreamHasher64&) = delete;
    StreamHasher64& operator=(const StreamHasher64&) = delete;

    void reset() {
        XXH3_64bits_reset(state_);
    }

    void update(const void* data, size_t len) {
        XXH3_64bits_update(state_, data, len);
    }

    u32 digest32() const {
        return (u32)(XXH3_64bits_digest(state_) & 0xFFFFFFFFull);
    }

private:
    XXH3_state_t* state_;
};

// Compare two Hash128 values
inline bool equal(const Hash128& a, const Hash128& b) {
    return a == b;
}

// Convert Hash128 to raw bytes (copy to 16-byte array)
inline void to_bytes(const Hash128& h, u8 out[16]) {
    std::copy(h.begin(), h.end(), out);
}

// Build Hash128 from raw bytes
inline Hash128 from_bytes(const u8 in[16]) {
    Hash128 h;
    std::copy(in, in + 16, h.begin());
    return h;
}

// ---- Adler-32 (weak rolling checksum for delta-sync) ----
// Standard Adler-32: MOD_ADLER = 65521
inline u32 adler32(const void* data, size_t len) {
    const u8* p = static_cast<const u8*>(data);
    u32 a = 1, b = 0;
    constexpr u32 MOD = 65521;
    for (size_t i = 0; i < len; ++i) {
        a = (a + p[i]) % MOD;
        b = (b + a)    % MOD;
    }
    return (b << 16) | a;
}

} // namespace hash
