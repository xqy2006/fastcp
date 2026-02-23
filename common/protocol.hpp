#pragma once

// protocol.hpp -- Wire protocol definitions for FastCP

#include "platform.hpp"
#include <cstring>

// Magic number: "FCP1"
static constexpr u32 FASTCP_MAGIC = 0x46435031u;
static constexpr u8  FASTCP_VERSION = 1;

static constexpr u32 MAX_PAYLOAD_LEN   = 64u * 1024u * 1024u;
static constexpr u32 DEFAULT_CHUNK_SIZE = 1u * 1024u * 1024u;
static constexpr u64 BUNDLE_THRESHOLD  = 64u * 1024u;
static constexpr u32 MAX_BUNDLE_SIZE   = 4u * 1024u * 1024u;
static constexpr u32 MAX_BUNDLE_FILES  = 256u;

// ---- Message Types (all prefixed MT_ to avoid Windows macro collisions) ----
enum class MsgType : u16 {
    MT_HANDSHAKE_REQ  = 0x0001,
    MT_HANDSHAKE_ACK  = 0x0002,
    MT_HANDSHAKE_NACK = 0x0003,

    MT_FILE_LIST_BEGIN = 0x0010,
    MT_FILE_LIST_ENTRY = 0x0011,
    MT_FILE_LIST_END   = 0x0012,

    MT_SYNC_PLAN_BEGIN = 0x0020,
    MT_SYNC_PLAN_ENTRY = 0x0021,
    MT_SYNC_PLAN_END   = 0x0022,

    MT_FILE_META  = 0x0030,
    MT_FILE_CHUNK = 0x0031,
    MT_FILE_END   = 0x0032,

    MT_BUNDLE_BEGIN = 0x0040,
    MT_BUNDLE_ENTRY = 0x0041,
    MT_BUNDLE_END   = 0x0042,

    MT_ACK          = 0x0050,
    MT_NACK         = 0x0051,
    MT_SESSION_DONE = 0x0060,
    MT_PING         = 0x0070,
    MT_PONG         = 0x0071,
    MT_ERROR_MSG    = 0x00FF,

    // Delta sync: client sends block checksums so server can skip unchanged blocks
    MT_BLOCK_CHECKSUMS     = 0x0080,  // payload: BlockChecksumMsg header + BlockChecksumEntry[]
    MT_BLOCK_CHECKSUMS_END = 0x0081,  // no payload; signals end of checksum stream
};

// ---- Frame Header (8 bytes, big-endian on wire) ----
struct FrameHeader {
    u16 msg_type;
    u16 flags;
    u32 payload_len;
};
static_assert(sizeof(FrameHeader) == 8, "FrameHeader must be 8 bytes");

// ---- Sync action codes ----
enum class SyncAction : u8 {
    SKIP        = 0,
    FULL        = 1,
    PARTIAL     = 2,
    REMOVE_FILE = 3,
};

// ---- Capabilities bits ----
enum Capabilities : u16 {
    CAP_COMPRESS = 0x0001,
    CAP_RESUME   = 0x0002,
    CAP_BUNDLE   = 0x0004,
    CAP_DELTA    = 0x0008,  // delta-sync (block-level checksum exchange)
};

// ---- Compress algo ----
enum class CompressAlgo : u8 {
    NONE = 0,
    ZSTD = 1,
};

// ============================================================
// Packed structures (wire format, big-endian)
// ============================================================
#pragma pack(push, 1)

// HandshakeReq: 20 bytes
struct HandshakeReq {
    u8  magic[4];
    u8  version;
    u8  num_connections;
    u16 capabilities;
    u64 session_id;
    u16 conn_index;
    u8  pad[2];
};
static_assert(sizeof(HandshakeReq) == 20, "HandshakeReq size mismatch");

// HandshakeAck: 16 bytes
struct HandshakeAck {
    u64 session_id;
    u16 accepted_conns;
    u16 capabilities;
    u32 chunk_size_kb;
};
static_assert(sizeof(HandshakeAck) == 16, "HandshakeAck size mismatch");

// FileListEntry: 44 bytes fixed + path
struct FileListEntry {
    u32 file_id;
    u64 file_size;
    u64 mtime_ns;
    u8  xxh3_128[16];
    u16 path_len;
    u8  flags;
    u8  pad[5];
};
static_assert(sizeof(FileListEntry) == 44, "FileListEntry size mismatch");

// SyncPlanEntry: 20 bytes
struct SyncPlanEntry {
    u32 file_id;
    u8  action;
    u8  pad[3];
    u64 resume_offset;
    u32 reserved;
};
static_assert(sizeof(SyncPlanEntry) == 20, "SyncPlanEntry size mismatch");

// FileMeta: 56 bytes fixed + path
struct FileMeta {
    u32 file_id;
    u64 file_size;
    u64 mtime_ns;
    u8  xxh3[16];
    u32 chunk_count;
    u32 chunk_size;
    u8  compress_algo;
    u8  pad[3];
    u16 path_len;
    u8  pad2[6];
};
static_assert(sizeof(FileMeta) == 56, "FileMeta size mismatch");

// FileChunk: 32 bytes fixed + data
struct FileChunk {
    u32 file_id;
    u32 chunk_index;
    u32 data_len;
    u64 file_offset;
    u32 xxh3_32;
    u8  pad[8];
};
static_assert(sizeof(FileChunk) == 32, "FileChunk size mismatch");

// FileEnd: 32 bytes
struct FileEnd {
    u32 file_id;
    u8  xxh3_128[16];
    u64 total_size;
    u8  pad[4];
};
static_assert(sizeof(FileEnd) == 32, "FileEnd size mismatch");

// BundleBegin: 16 bytes
struct BundleBegin {
    u32 bundle_id;
    u16 file_count;
    u8  pad[2];
    u64 total_size;
};
static_assert(sizeof(BundleBegin) == 16, "BundleBegin size mismatch");

// BundleEntryHdr: 48 bytes fixed + path + data
struct BundleEntryHdr {
    u32 file_id;
    u64 file_size;
    u64 mtime_ns;
    u8  xxh3[16];
    u16 path_len;
    u8  pad[10];
};
static_assert(sizeof(BundleEntryHdr) == 48, "BundleEntryHdr size mismatch");

// Ack: 16 bytes
struct Ack {
    u32 ref_id;
    u16 ref_type;
    u16 chunk_index;
    u32 status;
    u8  pad[4];
};
static_assert(sizeof(Ack) == 16, "Ack size mismatch");

// Nack: 20 bytes
struct NackMsg {
    u32 ref_id;
    u16 ref_type;
    u16 chunk_index;
    u32 error_code;
    u8  pad[8];
};
static_assert(sizeof(NackMsg) == 20, "NackMsg size mismatch");

// ---- Delta sync structures ----

// BlockChecksumMsg header: 16 bytes
// Followed by block_count × BlockChecksumEntry
struct BlockChecksumMsg {
    u32 file_id;
    u32 block_count;    // number of BlockChecksumEntry following
    u32 block_size;     // block size in bytes used by client
    u32 reserved;
};
static_assert(sizeof(BlockChecksumMsg) == 16, "BlockChecksumMsg size mismatch");

// BlockChecksumEntry: 12 bytes per block
// adler32 = Adler-32 weak checksum (fast rolling check)
// xxh3_32 = xxHash3-32 strong checksum (used only when adler32 matches)
struct BlockChecksumEntry {
    u32 block_index;
    u32 adler32;
    u32 xxh3_32;
};
static_assert(sizeof(BlockChecksumEntry) == 12, "BlockChecksumEntry size mismatch");

#pragma pack(pop)

// ---- Inline helpers ----
inline void handshake_req_init(HandshakeReq& h, u64 session_id, u8 num_conns, u16 conn_idx, u16 caps) {
    h.magic[0] = 'F'; h.magic[1] = 'C'; h.magic[2] = 'P'; h.magic[3] = '1';
    h.version = FASTCP_VERSION;
    h.num_connections = num_conns;
    h.capabilities = caps;
    h.session_id = session_id;
    h.conn_index = conn_idx;
    h.pad[0] = h.pad[1] = 0;
}

inline bool handshake_req_valid_magic(const HandshakeReq& h) {
    return h.magic[0]=='F' && h.magic[1]=='C' && h.magic[2]=='P' && h.magic[3]=='1';
}
