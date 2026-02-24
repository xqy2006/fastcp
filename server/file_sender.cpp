// ============================================================
// file_sender.cpp -- Parallel file sending implementation
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
#include <condition_variable>

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

// ---- Legacy single-threaded send ----

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

        auto g = pool_.guard(meta_conn);
        g.socket().write_frame(MsgType::MT_FILE_META, 0, payload.data(), (u32)payload.size());
    }

    // Send chunks on the same connection as FileMeta (to maintain order)
    u64 delta_bytes_skipped = 0;
    for (u32 ci = start_chunk; ci < total_chunks; ++ci) {
        u64 offset = (u64)ci * chunk_size_;
        u32 raw_len = (u32)reader->chunk_len(offset, chunk_size_);
        if (raw_len == 0) break;

        const char* data_ptr = reader->chunk_ptr(offset);

        // Check if client already has this block (delta sync)
        if (block_matches_client(fe.file_id, ci, data_ptr, raw_len)) {
            delta_bytes_skipped += raw_len;
            tui_state_.bytes_sent.fetch_add(raw_len);
            continue;
        }

        // Use the assigned connection (meta_conn) for all chunks
        bool ok = false;
        {
            auto g = pool_.guard(meta_conn);
            ok = send_chunk(g.socket(), fe, ci, offset, data_ptr, raw_len, do_compress);
        }

        if (!ok) {
            bool success = false;
            for (int retry = 0; retry < 3 && !success; ++retry) {
                LOG_WARN("Retrying chunk " + std::to_string(ci) + " of " + fe.rel_path);
                auto g2 = pool_.guard(meta_conn);
                success = send_chunk(g2.socket(), fe, ci, offset, data_ptr, raw_len, do_compress);
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

        auto g = pool_.guard(meta_conn);
        g.socket().write_frame(MsgType::MT_FILE_END, 0, &encoded, sizeof(encoded));
    }

    return true;
}

// ---- Parallel send ----

bool FileSender::send_large_file_parallel(
    const FileEntry& fe,
    u64 resume_offset,
    const std::vector<int>& conn_indices,
    int num_threads_for_file,
    int thread_idx,
    FileDoneCallback on_done)
{
    // Each thread uses its own dedicated connection for data ordering
    if (thread_idx >= (int)conn_indices.size()) {
        LOG_ERROR("thread_idx out of range: " + std::to_string(thread_idx));
        return false;
    }
    int my_conn = conn_indices[thread_idx];

    // Update TUI current file (only first thread)
    if (thread_idx == 0) {
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
    u32 total_chunks = (u32)((fe.file_size + chunk_size_ - 1) / chunk_size_);

    // (file tracking is initialized by init_file_tracking() before threads are spawned)

    // Each thread sends FileMeta on its own connection (client ignores duplicates)
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

        std::vector<u8> payload(sizeof(FileMeta) + fe.rel_path.size());
        FileMeta encoded = meta;
        proto::encode_file_meta(encoded);
        std::memcpy(payload.data(), &encoded, sizeof(FileMeta));
        std::memcpy(payload.data() + sizeof(FileMeta),
                    fe.rel_path.data(), fe.rel_path.size());

        auto g = pool_.guard(my_conn);
        g.socket().write_frame(MsgType::MT_FILE_META, 0, payload.data(), (u32)payload.size());
    }

    // Each thread handles chunks where (chunk_index % num_threads) == thread_idx
    // Send on this thread's dedicated connection
    u64 delta_bytes_skipped = 0;
    bool thread_ok = true;

    for (u32 ci = thread_idx; ci < total_chunks; ci += (u32)num_threads_for_file) {
        u64 offset = (u64)ci * chunk_size_;
        u32 raw_len = (u32)reader->chunk_len(offset, chunk_size_);
        if (raw_len == 0) break;

        const char* data_ptr = reader->chunk_ptr(offset);

        // Delta sync check
        if (block_matches_client(fe.file_id, ci, data_ptr, raw_len)) {
            delta_bytes_skipped += raw_len;
            tui_state_.bytes_sent.fetch_add(raw_len);
            continue;
        }

        // Send chunk on this thread's dedicated connection
        bool ok = false;
        {
            auto g = pool_.guard(my_conn);
            ok = send_chunk(g.socket(), fe, ci, offset, data_ptr, raw_len, do_compress);
        }

        if (!ok) {
            bool success = false;
            for (int retry = 0; retry < 3 && !success; ++retry) {
                auto g2 = pool_.guard(my_conn);
                success = send_chunk(g2.socket(), fe, ci, offset, data_ptr, raw_len, do_compress);
            }
            if (!success) {
                Logger::get().transfer_error("Chunk " + std::to_string(ci) +
                    " of " + fe.rel_path + " failed after 3 retries");
                thread_ok = false;
            }
        }

        tui_state_.bytes_sent.fetch_add(raw_len);
    }

    if (delta_bytes_skipped > 0) {
        LOG_DEBUG("Delta[" + std::to_string(thread_idx) + "]: skipped " +
                  utils::format_bytes(delta_bytes_skipped) + " of " + fe.rel_path);
    }

    // Coordinate: last thread sends FILE_END.
    // IMPORTANT: Release file_completion_mutex_ BEFORE acquiring pool guard to avoid deadlock.
    // (Lock order inversion: another thread may hold pool guard while waiting for completion mutex)
    bool is_last = false;
    {
        std::lock_guard<std::mutex> lk(file_completion_mutex_);
        if (!thread_ok) {
            file_success_[fe.file_id] = false;
        }
        is_last = (--file_threads_pending_[fe.file_id] == 0);
        if (is_last) {
            // Cleanup map entries while holding the lock
            file_threads_pending_.erase(fe.file_id);
            file_success_.erase(fe.file_id);
            file_chunks_pending_.erase(fe.file_id);
        }
    }
    // file_completion_mutex_ released here — safe to acquire pool guard now

    if (is_last) {
        int end_conn = conn_indices[0];

        FileEnd end_msg{};
        end_msg.file_id    = fe.file_id;
        end_msg.total_size = fe.file_size;
        hash::to_bytes(fe.xxh3_128, end_msg.xxh3_128);

        FileEnd encoded = end_msg;
        proto::encode_file_end(encoded);

        {
            auto g = pool_.guard(end_conn);
            g.socket().write_frame(MsgType::MT_FILE_END, 0, &encoded, sizeof(encoded));
        }

        if (on_done) {
            on_done(fe.file_id);
        }
    }

    return thread_ok;
}

// ---- Bundle ----

bool FileSender::send_bundle(const std::vector<const FileEntry*>& files, int conn_idx) {
    if (files.empty()) return true;

    auto g = pool_.guard(conn_idx);
    TcpSocket& sock = g.socket();

    // Send BUNDLE_BEGIN with file count and total size
    u64 total_size = 0;
    for (auto* fe : files) total_size += fe->file_size;

    BundleBegin bb{};
    bb.bundle_id   = 0;
    bb.file_count  = (u16)files.size();
    bb.pad[0] = bb.pad[1] = 0;
    bb.total_size  = total_size;
    proto::encode_bundle_begin(bb);
    sock.write_frame(MsgType::MT_BUNDLE_BEGIN, 0, &bb, sizeof(bb));

    bool all_ok = true;
    for (auto* fe : files) {
        // Read file
        std::unique_ptr<file_io::MmapReader> reader;
        try {
            reader = std::make_unique<file_io::MmapReader>(fe->abs_path);
        } catch (const std::exception& e) {
            Logger::get().transfer_error("Cannot read " + fe->abs_path + ": " + e.what());
            all_ok = false;
            continue;
        }

        // Send BUNDLE_ENTRY_HDR + path + data
        BundleEntryHdr entry{};
        entry.file_id   = fe->file_id;
        entry.file_size = fe->file_size;
        entry.mtime_ns  = fe->mtime_ns;
        entry.path_len  = (u16)fe->rel_path.size();
        hash::to_bytes(fe->xxh3_128, entry.xxh3);

        std::vector<u8> payload(sizeof(BundleEntryHdr) + fe->rel_path.size() + fe->file_size);
        BundleEntryHdr encoded = entry;
        proto::encode_bundle_entry_hdr(encoded);
        std::memcpy(payload.data(), &encoded, sizeof(BundleEntryHdr));
        std::memcpy(payload.data() + sizeof(BundleEntryHdr),
                    fe->rel_path.data(), fe->rel_path.size());
        if (fe->file_size > 0) {
            std::memcpy(payload.data() + sizeof(BundleEntryHdr) + fe->rel_path.size(),
                        reader->data(), fe->file_size);
        }

        sock.write_frame(MsgType::MT_BUNDLE_ENTRY, 0, payload.data(), (u32)payload.size());
        tui_state_.bytes_sent.fetch_add(fe->file_size);
    }

    // Send BUNDLE_END
    sock.write_frame(MsgType::MT_BUNDLE_END, 0, nullptr, 0);

    return all_ok;
}

// ---- Chunk send ----

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
        if (comp_buf.size() < raw_len) {
            send_data = comp_buf.data();
            send_len  = (u32)comp_buf.size();
            actually_compressed = true;
        }
    }

    u32 xxh = hash::xxh3_32(data, raw_len);

    FileChunk chunk{};
    chunk.file_id     = fe.file_id;
    chunk.chunk_index = chunk_index;
    chunk.data_len    = send_len;
    chunk.file_offset = offset;
    chunk.xxh3_32     = xxh;
    chunk.pad[0]      = actually_compressed ? 1 : 0;

    std::vector<u8> payload(sizeof(FileChunk) + send_len);
    FileChunk encoded = chunk;
    proto::encode_file_chunk(encoded);
    std::memcpy(payload.data(), &encoded, sizeof(FileChunk));
    std::memcpy(payload.data() + sizeof(FileChunk), send_data, send_len);

    try {
        sock.write_frame(MsgType::MT_FILE_CHUNK, 0, payload.data(), (u32)payload.size());
        return true;
    } catch (const std::exception& e) {
        LOG_WARN("send_chunk failed: " + std::string(e.what()));
        return false;
    }
}

// ---- Delta sync check ----

bool FileSender::block_matches_client(u32 file_id, u32 block_index,
                                       const char* data, u32 len) const
{
    auto it = delta_checksums_.find(file_id);
    if (it == delta_checksums_.end()) return false;

    for (const auto& entry : it->second) {
        if (entry.block_index == block_index) {
            u32 adler = hash::adler32(data, len);
            if (adler != entry.adler32) return false;
            u32 strong = hash::xxh3_32(data, len);
            return strong == entry.xxh3_32;
        }
    }
    return false;
}

// ---- Per-file tracking initialization ----

void FileSender::init_file_tracking(u32 file_id, int num_threads) {
    std::lock_guard<std::mutex> lk(file_completion_mutex_);
    file_threads_pending_[file_id] = num_threads;
    file_success_[file_id] = true;
}

// ---- ACK/NACK handling ----

bool FileSender::on_ack(const Ack& ack) {
    // Could track acknowledged chunks here for flow control
    (void)ack;
    return true;
}

bool FileSender::on_nack(const NackMsg& nack, const FileEntry* /*fe*/) {
    std::lock_guard<std::mutex> lk(retry_mutex_);
    pending_retries_.push_back({nack.ref_id, nack.chunk_index, 0});
    return true;
}

bool FileSender::flush_acks() {
    // Process pending retries
    std::vector<ChunkRetry> retries;
    {
        std::lock_guard<std::mutex> lk(retry_mutex_);
        retries = std::move(pending_retries_);
        pending_retries_.clear();
    }

    // Note: actual retry logic would require access to file data
    // For now, just count errors
    if (!retries.empty()) {
        nack_errors_.fetch_add((int)retries.size());
    }
    return true;
}
