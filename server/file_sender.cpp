// ============================================================
// file_sender.cpp
// ============================================================

#include "file_sender.hpp"
#include "../common/protocol_io.hpp"
#include "../common/compress.hpp"
#include "../common/hash.hpp"
#include "../common/file_io.hpp"
#include "../common/logger.hpp"
#include "../common/utils.hpp"
#include <cstring>
#include <vector>
#include <mutex>

FileSender::FileSender(ConnectionPool& pool,
                       TuiState& tui_state,
                       bool use_compress,
                       u32  chunk_size,
                       const DeltaChecksumMap&  delta_checksums,
                       const DeltaBlockSizeMap& delta_block_sizes)
    : pool_(pool)
    , tui_state_(tui_state)
    , use_compress_(use_compress)
    , chunk_size_(chunk_size)
    , delta_checksums_(delta_checksums)
    , delta_block_sizes_(delta_block_sizes)
{}

bool FileSender::send_large_file(const FileEntry& fe, u64 resume_offset, int conn_idx) {
    // Update TUI current file
    {
        std::lock_guard<std::mutex> lk(tui_state_.current_file_mutex);
        tui_state_.current_file = fe.rel_path;
    }

    bool do_compress = use_compress_ && compress::should_compress(fe.rel_path);

    // Open file
    std::unique_ptr<file_io::MmapReader> reader;
    if (fe.file_size > 0) {
        try {
            reader = std::make_unique<file_io::MmapReader>(fe.abs_path);
        } catch (const std::exception& e) {
            Logger::get().transfer_error("Cannot read file " + fe.abs_path + ": " + e.what());
            return false;
        }
    }

    // Calculate chunks
    u64 start_offset = resume_offset;
    u32 total_chunks = (u32)((fe.file_size + chunk_size_ - 1) / chunk_size_);
    u32 start_chunk  = (u32)(start_offset / chunk_size_);

    // Determine which connection to use for FileMeta
    int meta_conn = (conn_idx >= 0) ? conn_idx : (pool_.next_index());

    // Send FileMeta on first/assigned connection
    {
        FileMeta meta{};
        meta.file_id     = fe.file_id;
        meta.file_size   = fe.file_size;
        meta.mtime_ns    = fe.mtime_ns;
        meta.chunk_count = total_chunks;
        meta.chunk_size  = chunk_size_;
        meta.compress_algo = do_compress ? (u8)CompressAlgo::ZSTD : (u8)CompressAlgo::NONE;
        meta.path_len    = (u16)fe.rel_path.size();
        hash::to_bytes(fe.xxh3_128, meta.xxh3);

        // Build payload = FileMeta + path
        std::vector<u8> payload(sizeof(FileMeta) + fe.rel_path.size());
        FileMeta encoded = meta;
        proto::encode_file_meta(encoded);
        std::memcpy(payload.data(), &encoded, sizeof(FileMeta));
        std::memcpy(payload.data() + sizeof(FileMeta),
                    fe.rel_path.data(), fe.rel_path.size());

        TcpSocket& sock = pool_.checkout(meta_conn);
        sock.write_frame(MsgType::MT_FILE_META, 0, payload.data(), (u32)payload.size());
        pool_.checkin(meta_conn);
    }

    // Send chunks round-robin across all connections
    // Delta sync: skip chunks where client already has identical data
    int num_conns = pool_.size();
    u64 delta_bytes_skipped = 0;
    for (u32 ci = start_chunk; ci < total_chunks; ++ci) {
        u64 offset = (u64)ci * chunk_size_;
        u32 raw_len = (u32)reader->chunk_len(offset, chunk_size_);
        if (raw_len == 0) break;

        const char* data_ptr = reader->chunk_ptr(offset);

        // Check if client already has this block (delta sync)
        if (block_matches_client(fe.file_id, ci, data_ptr, raw_len)) {
            delta_bytes_skipped += raw_len;
            tui_state_.bytes_sent.fetch_add(raw_len); // count as "sent" for progress
            continue; // client can reuse its local copy
        }

        int target_conn = ci % (u32)num_conns;

        bool ok = send_chunk(pool_.get(target_conn), fe, ci, offset, data_ptr, raw_len, do_compress);
        if (!ok) {
            // Retry up to 3 times
            bool success = false;
            for (int retry = 0; retry < 3 && !success; ++retry) {
                LOG_WARN("Retrying chunk " + std::to_string(ci) + " of " + fe.rel_path);
                success = send_chunk(pool_.get(target_conn), fe, ci, offset, data_ptr, raw_len, do_compress);
            }
            if (!success) {
                Logger::get().transfer_error("Chunk " + std::to_string(ci) +
                    " of " + fe.rel_path + " failed after 3 retries");
                return false;
            }
        }

        tui_state_.bytes_sent.fetch_add(raw_len);
    }

    if (delta_bytes_skipped > 0) {
        LOG_DEBUG("Delta: skipped " + utils::format_bytes(delta_bytes_skipped) +
                  " of " + fe.rel_path + " (unchanged blocks)");
    }

    // Send FILE_END
    {
        FileEnd end_msg{};
        end_msg.file_id    = fe.file_id;
        end_msg.total_size = fe.file_size;
        hash::to_bytes(fe.xxh3_128, end_msg.xxh3_128);

        FileEnd encoded = end_msg;
        proto::encode_file_end(encoded);

        TcpSocket& sock = pool_.checkout(meta_conn);
        sock.write_frame(MsgType::MT_FILE_END, 0, &encoded, sizeof(encoded));
        pool_.checkin(meta_conn);
    }

    return true;
}

bool FileSender::send_chunk(TcpSocket& sock,
                             const FileEntry& fe,
                             u32 chunk_index,
                             u64 offset,
                             const char* data,
                             u32 raw_len,
                             bool do_compress)
{
    std::vector<u8> comp_buf;
    const u8* send_data = (const u8*)data;
    u32 send_len = raw_len;
    bool actually_compressed = false;

    if (do_compress) {
        comp_buf = compress::compress_to_vec(data, raw_len);
        // Only use compressed if it's actually smaller
        if (comp_buf.size() < raw_len) {
            send_data = comp_buf.data();
            send_len  = (u32)comp_buf.size();
            actually_compressed = true;
        }
    }

    u32 xxh = hash::xxh3_32(data, raw_len); // always hash raw data

    FileChunk chunk{};
    chunk.file_id     = fe.file_id;
    chunk.chunk_index = chunk_index;
    chunk.data_len    = send_len;
    chunk.file_offset = offset;
    chunk.xxh3_32     = xxh;
    chunk.pad[0]      = actually_compressed ? 1 : 0; // per-chunk compression flag

    // Build payload = FileChunk + data
    std::vector<u8> payload(sizeof(FileChunk) + send_len);
    FileChunk encoded = chunk;
    proto::encode_file_chunk(encoded);
    std::memcpy(payload.data(), &encoded, sizeof(FileChunk));
    std::memcpy(payload.data() + sizeof(FileChunk), send_data, send_len);

    try {
        sock.write_frame(MsgType::MT_FILE_CHUNK, 0, payload.data(), (u32)payload.size());

        // Non-blocking check for NACK (select with timeout=0).
        // The server only sends a frame on this connection when a chunk fails,
        // so this is a no-op on the happy path (no extra RTT).
        fd_set rset;
        FD_ZERO(&rset);
        FD_SET(sock.native(), &rset);
        struct timeval tv = {0, 0};
        int r = select((int)sock.native() + 1, &rset, nullptr, nullptr, &tv);
        if (r > 0) {
            FrameHeader nack_hdr{};
            std::vector<u8> nack_payload;
            if (sock.read_frame(nack_hdr, nack_payload)) {
                if ((MsgType)nack_hdr.msg_type == MsgType::MT_NACK) {
                    return false; // caller retries up to 3 times
                }
            }
        }
        return true;
    } catch (const std::exception& e) {
        LOG_ERROR("send_chunk failed: " + std::string(e.what()));
        return false;
    }
}

bool FileSender::send_bundle(const std::vector<const FileEntry*>& files, int conn_idx) {
    if (files.empty()) return true;

    static std::atomic<u32> bundle_counter{1};
    u32 bundle_id = bundle_counter.fetch_add(1);

    // Calculate total payload
    u64 total_size = 0;
    for (auto* fe : files) total_size += fe->file_size;

    TcpSocket& sock = pool_.checkout(conn_idx);

    try {
        // Send BUNDLE_BEGIN
        {
            BundleBegin bb{};
            bb.bundle_id  = bundle_id;
            bb.file_count = (u16)files.size();
            bb.total_size = total_size;
            BundleBegin encoded = bb;
            proto::encode_bundle_begin(encoded);
            sock.write_frame(MsgType::MT_BUNDLE_BEGIN, 0, &encoded, sizeof(encoded));
        }

        // Send each entry
        for (auto* fe : files) {
            // Read file data
            std::vector<u8> data;
            if (fe->file_size > 0) {
                data = file_io::read_small_file(fe->abs_path);
            }

            BundleEntryHdr hdr{};
            hdr.file_id   = fe->file_id;
            hdr.file_size = fe->file_size;
            hdr.mtime_ns  = fe->mtime_ns;
            hdr.path_len  = (u16)fe->rel_path.size();
            hash::to_bytes(fe->xxh3_128, hdr.xxh3);

            // Payload = header + path + data
            size_t total_payload = sizeof(BundleEntryHdr) + fe->rel_path.size() + data.size();
            std::vector<u8> payload(total_payload);

            BundleEntryHdr encoded_hdr = hdr;
            proto::encode_bundle_entry_hdr(encoded_hdr);
            size_t off = 0;
            std::memcpy(payload.data() + off, &encoded_hdr, sizeof(BundleEntryHdr));
            off += sizeof(BundleEntryHdr);
            std::memcpy(payload.data() + off, fe->rel_path.data(), fe->rel_path.size());
            off += fe->rel_path.size();
            if (!data.empty()) {
                std::memcpy(payload.data() + off, data.data(), data.size());
            }

            sock.write_frame(MsgType::MT_BUNDLE_ENTRY, 0, payload.data(), (u32)payload.size());
            tui_state_.bytes_sent.fetch_add(fe->file_size);
        }

        // Send BUNDLE_END
        sock.write_frame(MsgType::MT_BUNDLE_END, 0, nullptr, 0);

    } catch (const std::exception& e) {
        pool_.checkin(conn_idx);
        LOG_ERROR("send_bundle failed: " + std::string(e.what()));
        return false;
    }

    pool_.checkin(conn_idx);
    return true;
}

bool FileSender::on_ack(const Ack& ack) {
    (void)ack;
    return true;
}

bool FileSender::on_nack(const NackMsg& nack, const FileEntry* fe) {
    if (!fe) return false;
    std::lock_guard<std::mutex> lk(retry_mutex_);
    ChunkRetry cr;
    cr.file_id     = nack.ref_id;
    cr.chunk_index = nack.chunk_index;
    cr.retries     = 0;
    pending_retries_.push_back(cr);
    return true;
}

bool FileSender::flush_acks() {
    return nack_errors_.load() == 0;
}

// ---- Delta sync: check if server's block matches client's local copy ----

bool FileSender::block_matches_client(u32 file_id, u32 block_index,
                                       const char* data, u32 len) const
{
    auto it = delta_checksums_.find(file_id);
    if (it == delta_checksums_.end()) return false;

    auto& entries = it->second;
    // Binary search by block_index (entries are sorted by block_index from client)
    // Client sends them in order so index == position; but do a scan for safety.
    for (auto& entry : entries) {
        if (entry.block_index == block_index) {
            // First check weak checksum (fast)
            u32 adler = hash::adler32(data, len);
            if (adler != entry.adler32) return false;
            // Then check strong checksum
            u32 strong = hash::xxh3_32(data, len);
            return strong == entry.xxh3_32;
        }
    }
    return false;
}
