#pragma once

// ============================================================
// protocol_io.hpp -- Frame read/write with byte-order handling
// ============================================================

#include "protocol.hpp"
#include <vector>
#include <stdexcept>

// Linux: htobe16/32/64 and be16/32/64toh live in <endian.h>
#ifndef _WIN32
#  include <endian.h>
#endif

// Forward declaration
class TcpSocket;

namespace proto {

// ---- Byte-order helpers ----

inline u16 hton16(u16 v) {
#if defined(_WIN32)
    return htons(v);
#else
    return htobe16(v);
#endif
}

inline u32 hton32(u32 v) {
#if defined(_WIN32)
    return htonl(v);
#else
    return htobe32(v);
#endif
}

inline u64 hton64(u64 v) {
#if defined(_WIN32)
    // Windows doesn't have htonll before VS2013 reliably
    return (((u64)htonl((u32)(v & 0xFFFFFFFFull))) << 32) | htonl((u32)(v >> 32));
#else
    return htobe64(v);
#endif
}

inline u16 ntoh16(u16 v) {
#if defined(_WIN32)
    return ntohs(v);
#else
    return be16toh(v);
#endif
}

inline u32 ntoh32(u32 v) {
#if defined(_WIN32)
    return ntohl(v);
#else
    return be32toh(v);
#endif
}

inline u64 ntoh64(u64 v) {
#if defined(_WIN32)
    return (((u64)ntohl((u32)(v & 0xFFFFFFFFull))) << 32) | ntohl((u32)(v >> 32));
#else
    return be64toh(v);
#endif
}

// ---- Serialise / deserialise FrameHeader ----

inline void encode_header(const FrameHeader& h, u8 buf[8]) {
    u16 mt = hton16(h.msg_type);
    u16 fl = hton16(h.flags);
    u32 pl = hton32(h.payload_len);
    std::memcpy(buf,     &mt, 2);
    std::memcpy(buf + 2, &fl, 2);
    std::memcpy(buf + 4, &pl, 4);
}

inline FrameHeader decode_header(const u8 buf[8]) {
    FrameHeader h;
    u16 mt, fl; u32 pl;
    std::memcpy(&mt, buf,     2);
    std::memcpy(&fl, buf + 2, 2);
    std::memcpy(&pl, buf + 4, 4);
    h.msg_type    = ntoh16(mt);
    h.flags       = ntoh16(fl);
    h.payload_len = ntoh32(pl);
    return h;
}

// ---- Encode individual struct fields (in-place, host->network) ----

inline void encode_handshake_req(HandshakeReq& h) {
    // magic, version, num_connections: single bytes, no swap needed
    h.capabilities = hton16(h.capabilities);
    h.session_id   = hton64(h.session_id);
    h.conn_index   = hton16(h.conn_index);
}

inline void decode_handshake_req(HandshakeReq& h) {
    h.capabilities = ntoh16(h.capabilities);
    h.session_id   = ntoh64(h.session_id);
    h.conn_index   = ntoh16(h.conn_index);
}

inline void encode_handshake_ack(HandshakeAck& h) {
    h.session_id     = hton64(h.session_id);
    h.accepted_conns = hton16(h.accepted_conns);
    h.capabilities   = hton16(h.capabilities);
    h.chunk_size_kb  = hton32(h.chunk_size_kb);
}

inline void decode_handshake_ack(HandshakeAck& h) {
    h.session_id     = ntoh64(h.session_id);
    h.accepted_conns = ntoh16(h.accepted_conns);
    h.capabilities   = ntoh16(h.capabilities);
    h.chunk_size_kb  = ntoh32(h.chunk_size_kb);
}

inline void encode_file_list_entry(FileListEntry& e) {
    e.file_id  = hton32(e.file_id);
    e.file_size = hton64(e.file_size);
    e.mtime_ns  = hton64(e.mtime_ns);
    e.path_len  = hton16(e.path_len);
}

inline void decode_file_list_entry(FileListEntry& e) {
    e.file_id   = ntoh32(e.file_id);
    e.file_size = ntoh64(e.file_size);
    e.mtime_ns  = ntoh64(e.mtime_ns);
    e.path_len  = ntoh16(e.path_len);
}

inline void encode_sync_plan_entry(SyncPlanEntry& e) {
    e.file_id       = hton32(e.file_id);
    e.resume_offset = hton64(e.resume_offset);
    e.reserved      = hton32(e.reserved);
}

inline void decode_sync_plan_entry(SyncPlanEntry& e) {
    e.file_id       = ntoh32(e.file_id);
    e.resume_offset = ntoh64(e.resume_offset);
    e.reserved      = ntoh32(e.reserved);
}

inline void encode_file_meta(FileMeta& m) {
    m.file_id     = hton32(m.file_id);
    m.file_size   = hton64(m.file_size);
    m.mtime_ns    = hton64(m.mtime_ns);
    m.chunk_count = hton32(m.chunk_count);
    m.chunk_size  = hton32(m.chunk_size);
    m.path_len    = hton16(m.path_len);
}

inline void decode_file_meta(FileMeta& m) {
    m.file_id     = ntoh32(m.file_id);
    m.file_size   = ntoh64(m.file_size);
    m.mtime_ns    = ntoh64(m.mtime_ns);
    m.chunk_count = ntoh32(m.chunk_count);
    m.chunk_size  = ntoh32(m.chunk_size);
    m.path_len    = ntoh16(m.path_len);
}

inline void encode_file_chunk(FileChunk& c) {
    c.file_id     = hton32(c.file_id);
    c.chunk_index = hton32(c.chunk_index);
    c.data_len    = hton32(c.data_len);
    c.file_offset = hton64(c.file_offset);
    c.xxh3_32     = hton32(c.xxh3_32);
}

inline void decode_file_chunk(FileChunk& c) {
    c.file_id     = ntoh32(c.file_id);
    c.chunk_index = ntoh32(c.chunk_index);
    c.data_len    = ntoh32(c.data_len);
    c.file_offset = ntoh64(c.file_offset);
    c.xxh3_32     = ntoh32(c.xxh3_32);
}

inline void encode_file_end(FileEnd& e) {
    e.file_id    = hton32(e.file_id);
    e.total_size = hton64(e.total_size);
}

inline void decode_file_end(FileEnd& e) {
    e.file_id    = ntoh32(e.file_id);
    e.total_size = ntoh64(e.total_size);
}

inline void encode_bundle_begin(BundleBegin& b) {
    b.bundle_id  = hton32(b.bundle_id);
    b.file_count = hton16(b.file_count);
    b.total_size = hton64(b.total_size);
}

inline void decode_bundle_begin(BundleBegin& b) {
    b.bundle_id  = ntoh32(b.bundle_id);
    b.file_count = ntoh16(b.file_count);
    b.total_size = ntoh64(b.total_size);
}

inline void encode_bundle_entry_hdr(BundleEntryHdr& h) {
    h.file_id   = hton32(h.file_id);
    h.file_size = hton64(h.file_size);
    h.mtime_ns  = hton64(h.mtime_ns);
    h.path_len  = hton16(h.path_len);
}

inline void decode_bundle_entry_hdr(BundleEntryHdr& h) {
    h.file_id   = ntoh32(h.file_id);
    h.file_size = ntoh64(h.file_size);
    h.mtime_ns  = ntoh64(h.mtime_ns);
    h.path_len  = ntoh16(h.path_len);
}

inline void encode_ack(Ack& a) {
    a.ref_id      = hton32(a.ref_id);
    a.ref_type    = hton16(a.ref_type);
    a.chunk_index = hton16(a.chunk_index);
    a.status      = hton32(a.status);
}

inline void decode_ack(Ack& a) {
    a.ref_id      = ntoh32(a.ref_id);
    a.ref_type    = ntoh16(a.ref_type);
    a.chunk_index = ntoh16(a.chunk_index);
    a.status      = ntoh32(a.status);
}

inline void encode_nack(NackMsg& n) {
    n.ref_id      = hton32(n.ref_id);
    n.ref_type    = hton16(n.ref_type);
    n.chunk_index = hton16(n.chunk_index);
    n.error_code  = hton32(n.error_code);
}

inline void decode_nack(NackMsg& n) {
    n.ref_id      = ntoh32(n.ref_id);
    n.ref_type    = ntoh16(n.ref_type);
    n.chunk_index = ntoh16(n.chunk_index);
    n.error_code  = ntoh32(n.error_code);
}

// ---- Delta sync ----

inline void encode_block_checksum_msg(BlockChecksumMsg& m) {
    m.file_id     = hton32(m.file_id);
    m.block_count = hton32(m.block_count);
    m.block_size  = hton32(m.block_size);
    m.reserved    = hton32(m.reserved);
}

inline void decode_block_checksum_msg(BlockChecksumMsg& m) {
    m.file_id     = ntoh32(m.file_id);
    m.block_count = ntoh32(m.block_count);
    m.block_size  = ntoh32(m.block_size);
    m.reserved    = ntoh32(m.reserved);
}

inline void encode_block_checksum_entry(BlockChecksumEntry& e) {
    e.block_index = hton32(e.block_index);
    e.adler32     = hton32(e.adler32);
    e.xxh3_32     = hton32(e.xxh3_32);
}

inline void decode_block_checksum_entry(BlockChecksumEntry& e) {
    e.block_index = ntoh32(e.block_index);
    e.adler32     = ntoh32(e.adler32);
    e.xxh3_32     = ntoh32(e.xxh3_32);
}

// ---- Virtual Archive ----

inline void encode_archive_manifest_hdr(ArchiveManifestHdr& h) {
    h.total_virtual_size = hton64(h.total_virtual_size);
    h.total_files        = hton32(h.total_files);
    h.chunk_size         = hton32(h.chunk_size);
    h.total_chunks       = hton32(h.total_chunks);
}

inline void decode_archive_manifest_hdr(ArchiveManifestHdr& h) {
    h.total_virtual_size = ntoh64(h.total_virtual_size);
    h.total_files        = ntoh32(h.total_files);
    h.chunk_size         = ntoh32(h.chunk_size);
    h.total_chunks       = ntoh32(h.total_chunks);
}

inline void encode_archive_file_entry(ArchiveFileEntry& e) {
    e.file_id        = hton32(e.file_id);
    e.virtual_offset = hton64(e.virtual_offset);
    e.file_size      = hton64(e.file_size);
    e.mtime_ns       = hton64(e.mtime_ns);
    e.path_len       = hton16(e.path_len);
}

inline void decode_archive_file_entry(ArchiveFileEntry& e) {
    e.file_id        = ntoh32(e.file_id);
    e.virtual_offset = ntoh64(e.virtual_offset);
    e.file_size      = ntoh64(e.file_size);
    e.mtime_ns       = ntoh64(e.mtime_ns);
    e.path_len       = ntoh16(e.path_len);
}

inline void encode_chunk_request_hdr(ChunkRequestHdr& h) {
    h.needed_count = hton32(h.needed_count);
}

inline void decode_chunk_request_hdr(ChunkRequestHdr& h) {
    h.needed_count = ntoh32(h.needed_count);
}

inline void encode_archive_chunk_hdr(ArchiveChunkHdr& h) {
    h.chunk_id       = hton32(h.chunk_id);
    h.archive_offset = hton64(h.archive_offset);
    h.data_len       = hton32(h.data_len);
    h.raw_len        = hton32(h.raw_len);
    h.xxh3_32        = hton32(h.xxh3_32);
}

inline void decode_archive_chunk_hdr(ArchiveChunkHdr& h) {
    h.chunk_id       = ntoh32(h.chunk_id);
    h.archive_offset = ntoh64(h.archive_offset);
    h.data_len       = ntoh32(h.data_len);
    h.raw_len        = ntoh32(h.raw_len);
    h.xxh3_32        = ntoh32(h.xxh3_32);
}

} // namespace proto
