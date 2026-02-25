// ============================================================
// file_sender.cpp -- Parallel file sending implementation
// ============================================================

#include "file_sender.hpp"
#include "../common/protocol_io.hpp"
#include "../common/compress.hpp"
#include "../common/hash.hpp"
#include "../common/file_io.hpp"
#include "../common/logger.hpp"
#include "../common/write_buffer.hpp"
#include "../common/utils.hpp"
#include <cstring>
#include <vector>
#include <mutex>
#include <atomic>
#ifndef _WIN32
#  include <fcntl.h>
#  include <unistd.h>
#endif
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
    u32 total_chunks = (u32)((fe.file_size + chunk_size_ - 1) / chunk_size_);

    // Determine which connection to use for FileMeta
    int meta_conn = (conn_idx >= 0) ? conn_idx : (pool_.next_index());

    // Send FileMeta
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

        auto g = pool_.guard(meta_conn);
        g.socket().write_frame(MsgType::MT_FILE_META, 0, payload.data(), (u32)payload.size());
    }

    // ---- Chunk-level resume protocol ----
    // When enabled: send per-chunk hashes so the client can tell us which chunks
    // it already has. We then skip those chunks entirely instead of re-sending.
    // The entire hash-list + wait + reply loop happens while holding meta_conn so
    // ordering is guaranteed on this single connection.
    std::vector<u32> chunks_to_send;  // empty = send all from start_chunk

    if (use_chunk_resume_ && total_chunks > 0 && reader) {
        // Build CHUNK_HASH_LIST payload: header + chunk_count × u32 hashes
        std::vector<u8> hash_payload(sizeof(ChunkHashListHdr) +
                                     (size_t)total_chunks * sizeof(u32));

        ChunkHashListHdr cl_hdr{};
        cl_hdr.file_id     = fe.file_id;
        cl_hdr.chunk_count = total_chunks;
        cl_hdr.chunk_size  = chunk_size_;
        ChunkHashListHdr encoded_cl = cl_hdr;
        proto::encode_chunk_hash_list_hdr(encoded_cl);
        std::memcpy(hash_payload.data(), &encoded_cl, sizeof(ChunkHashListHdr));

        u8* hp = hash_payload.data() + sizeof(ChunkHashListHdr);
        for (u32 ci = 0; ci < total_chunks; ++ci) {
            u64 offset  = (u64)ci * chunk_size_;
            u32 raw_len = (u32)reader->chunk_len(offset, chunk_size_);
            u32 h = (raw_len > 0) ? hash::xxh3_32(reader->chunk_ptr(offset), raw_len) : 0;
            u32 h_net = proto::hton32(h);
            std::memcpy(hp + ci * sizeof(u32), &h_net, sizeof(u32));
        }

        // Send CHUNK_HASH_LIST and wait for FILE_CHUNK_REQUEST on the same connection
        {
            auto g = pool_.guard(meta_conn);
            g.socket().write_frame(MsgType::MT_CHUNK_HASH_LIST, 0,
                                   hash_payload.data(), (u32)hash_payload.size());

            // Read FILE_CHUNK_REQUEST (client tells us which chunks it needs)
            FrameHeader req_hdr{};
            std::vector<u8> req_payload;
            if (g.socket().read_frame(req_hdr, req_payload) &&
                (MsgType)req_hdr.msg_type == MsgType::MT_FILE_CHUNK_REQUEST &&
                req_payload.size() >= sizeof(FileChunkRequestHdr))
            {
                FileChunkRequestHdr creq{};
                std::memcpy(&creq, req_payload.data(), sizeof(FileChunkRequestHdr));
                proto::decode_file_chunk_request_hdr(creq);

                u32 needed = creq.needed_count;
                size_t expected = sizeof(FileChunkRequestHdr) + (size_t)needed * sizeof(u32);
                if (req_payload.size() >= expected) {
                    chunks_to_send.reserve(needed);
                    const u8* idx_ptr = req_payload.data() + sizeof(FileChunkRequestHdr);
                    for (u32 i = 0; i < needed; ++i) {
                        u32 v;
                        std::memcpy(&v, idx_ptr + i * sizeof(u32), sizeof(u32));
                        chunks_to_send.push_back(proto::ntoh32(v));
                    }
                    LOG_DEBUG("Chunk resume: sending " + std::to_string(needed) +
                              "/" + std::to_string(total_chunks) +
                              " chunks for " + fe.rel_path);
                }
            }
            // If we didn't get a valid response, fall through and send all chunks
        }
    }

    // If chunk resume gave us a specific list, use it; otherwise send all
    u32 start_chunk = (u32)(resume_offset / chunk_size_);
    bool use_chunk_list = !chunks_to_send.empty() ||
                          (use_chunk_resume_ && total_chunks > 0);

    u64 delta_bytes_skipped = 0;

    auto send_one_chunk = [&](u32 ci) -> bool {
        u64 offset = (u64)ci * chunk_size_;
        u32 raw_len = (u32)reader->chunk_len(offset, chunk_size_);
        if (raw_len == 0) return true;

        const char* data_ptr = reader->chunk_ptr(offset);

        // Check if client already has this block (delta sync)
        if (block_matches_client(fe.file_id, ci, data_ptr, raw_len)) {
            delta_bytes_skipped += raw_len;
            tui_state_.bytes_sent.fetch_add(raw_len);
            return true;
        }

        bool ok = false;
        {
            auto g = pool_.guard(meta_conn);
            ok = send_chunk(g.socket(), fe, ci, offset, data_ptr, raw_len, do_compress);
        }
        if (!ok) {
            for (int retry = 0; retry < 3 && !ok; ++retry) {
                LOG_WARN("Retrying chunk " + std::to_string(ci) + " of " + fe.rel_path);
                auto g2 = pool_.guard(meta_conn);
                ok = send_chunk(g2.socket(), fe, ci, offset, data_ptr, raw_len, do_compress);
            }
            if (!ok) {
                Logger::get().transfer_error("Chunk " + std::to_string(ci) +
                    " of " + fe.rel_path + " failed after 3 retries");
                return false;
            }
        }
        tui_state_.bytes_sent.fetch_add(raw_len);
        return true;
    };

    if (use_chunk_list && !chunks_to_send.empty()) {
        // Send only the chunks the client requested
        for (u32 ci : chunks_to_send) {
            if (!send_one_chunk(ci)) return false;
        }
    } else if (!use_chunk_resume_) {
        // Legacy path: send all chunks from start_chunk
        for (u32 ci = start_chunk; ci < total_chunks; ++ci) {
            if (!send_one_chunk(ci)) return false;
        }
    }
    // (use_chunk_resume_ && chunks_to_send is empty means client has all chunks → skip all)

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

// Parallel pre-read of small files into memory.
//
// Uses direct open/read/close syscalls (3 per file) instead of
// std::ifstream (which needs 5: open+seekg_end+tellg+seekg_beg+read).
// Since FileEntry already has the file size, we can skip the stat/seek.
// With 8 threads, this parallelises the per-file open() latency that is
// especially high on Docker overlayfs (~40 µs per file, 400 ms serial for
// 10 000 files → ~55 ms with 8 threads).
//
// After all workers finish the cache is read-only, so send_bundle() reads
// it without a lock.
void FileSender::prefill_small_cache(const std::vector<const FileEntry*>& files,
                                      int num_threads)
{
    if (files.empty()) return;

    std::mutex cache_mutex;
    std::atomic<size_t> next_idx{0};

    auto worker = [&]() {
        for (;;) {
            size_t i = next_idx.fetch_add(1, std::memory_order_relaxed);
            if (i >= files.size()) break;
            const FileEntry* fe = files[i];

            if (fe->file_size == 0) {
                std::lock_guard<std::mutex> lk(cache_mutex);
                small_file_cache_[fe->file_id] = {};
                continue;
            }

            std::vector<u8> buf((size_t)fe->file_size);
            bool ok = false;

#ifdef _WIN32
            HANDLE h = CreateFileA(fe->abs_path.c_str(),
                                   GENERIC_READ, FILE_SHARE_READ,
                                   nullptr, OPEN_EXISTING,
                                   FILE_ATTRIBUTE_NORMAL, nullptr);
            if (h != INVALID_HANDLE_VALUE) {
                DWORD n = 0;
                if (ReadFile(h, buf.data(), (DWORD)fe->file_size, &n, nullptr) &&
                    n == (DWORD)fe->file_size)
                    ok = true;
                CloseHandle(h);
            }
#else
            // 3 syscalls: open + read + close (no stat needed, size from FileEntry)
            int fd = ::open(fe->abs_path.c_str(), O_RDONLY);
            if (fd >= 0) {
                ssize_t n = ::read(fd, buf.data(), (size_t)fe->file_size);
                if (n == (ssize_t)fe->file_size) ok = true;
                ::close(fd);
            }
#endif
            if (ok) {
                std::lock_guard<std::mutex> lk(cache_mutex);
                small_file_cache_[fe->file_id] = std::move(buf);
            }
        }
    };

    int n = std::min(num_threads, (int)files.size());
    std::vector<std::thread> workers;
    workers.reserve((size_t)n);
    for (int i = 0; i < n; ++i) workers.emplace_back(worker);
    for (auto& w : workers) w.join();
}

void FileSender::clear_small_cache() {
    small_file_cache_.clear();
}

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

    // Use a write buffer: coalesces BUNDLE_BEGIN + N×BUNDLE_ENTRY + BUNDLE_END
    // into a few large send() calls instead of N+2 individual write() syscalls.
    TcpWriteBuffer wbuf(sock);
    wbuf.write_frame(MsgType::MT_BUNDLE_BEGIN, 0, &bb, sizeof(bb));

    bool all_ok = true;
    for (auto* fe : files) {
        // Resolve file content: prefer pre-read cache (no open/mmap overhead),
        // fall back to MmapReader when the cache wasn't populated.
        const u8* file_data = nullptr;
        std::unique_ptr<file_io::MmapReader> reader;

        auto cache_it = small_file_cache_.find(fe->file_id);
        if (cache_it != small_file_cache_.end()) {
            file_data = cache_it->second.empty() ? nullptr : cache_it->second.data();
        } else if (fe->file_size > 0) {
            try {
                reader = std::make_unique<file_io::MmapReader>(fe->abs_path);
                file_data = (const u8*)reader->data();
            } catch (const std::exception& e) {
                Logger::get().transfer_error("Cannot read " + fe->abs_path + ": " + e.what());
                all_ok = false;
                continue;
            }
        }

        // Build BUNDLE_ENTRY_HDR + path + data into one payload, then buffer it
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
        if (fe->file_size > 0 && file_data) {
            std::memcpy(payload.data() + sizeof(BundleEntryHdr) + fe->rel_path.size(),
                        file_data, fe->file_size);
        }

        wbuf.write_frame(MsgType::MT_BUNDLE_ENTRY, 0, payload.data(), (u32)payload.size());
        tui_state_.bytes_sent.fetch_add(fe->file_size);
    }

    // BUNDLE_END then explicit flush (destructor also guards)
    wbuf.write_frame(MsgType::MT_BUNDLE_END, 0, nullptr, 0);
    wbuf.flush();

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

    // Test-only chunk limit: abort session to simulate a crash
    if (max_chunks_ > 0) {
        int n = chunks_sent_.fetch_add(1) + 1;
        if (n > max_chunks_) {
            throw std::runtime_error(
                "CHUNK_LIMIT_REACHED (test mode, limit=" +
                std::to_string(max_chunks_) + ")");
        }
    }

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

// ---- Virtual Archive send ----

bool FileSender::send_archive_range(const ArchiveBuilder& archive,
                                     const std::vector<u32>& chunk_ids,
                                     int conn_idx)
{
    for (u32 chunk_id : chunk_ids) {
        if (chunk_id >= (u32)archive.chunks().size()) {
            LOG_WARN("send_archive_range: invalid chunk_id " + std::to_string(chunk_id));
            continue;
        }
        const VirtualChunk& vc = archive.chunks()[chunk_id];

        // Read raw chunk data (may span multiple files)
        std::vector<u8> raw_data;
        try {
            raw_data = archive.read_chunk(chunk_id);
        } catch (const std::exception& e) {
            LOG_WARN("send_archive_range: read_chunk " + std::to_string(chunk_id) +
                     " failed: " + e.what());
            return false;
        }

        // Optionally compress
        std::vector<u8> comp_buf;
        const u8* send_data   = raw_data.data();
        u32       send_len    = vc.raw_size;
        u8        comp_flag   = 0;

        if (use_compress_ && !raw_data.empty()) {
            comp_buf = compress::compress_to_vec(raw_data.data(), raw_data.size());
            if (comp_buf.size() < raw_data.size()) {
                send_data = comp_buf.data();
                send_len  = (u32)comp_buf.size();
                comp_flag = 1;
            }
        }

        // Compute xxh3_32 of raw data
        u32 xxh = raw_data.empty() ? 0 : hash::xxh3_32(raw_data.data(), raw_data.size());

        // Build header
        ArchiveChunkHdr hdr{};
        hdr.chunk_id       = chunk_id;
        hdr.archive_offset = vc.archive_offset;
        hdr.data_len       = send_len;
        hdr.raw_len        = vc.raw_size;
        hdr.xxh3_32        = xxh;
        hdr.compress_flag  = comp_flag;

        // Build payload = header + data
        std::vector<u8> payload(sizeof(ArchiveChunkHdr) + send_len);
        ArchiveChunkHdr encoded = hdr;
        proto::encode_archive_chunk_hdr(encoded);
        std::memcpy(payload.data(), &encoded, sizeof(ArchiveChunkHdr));
        if (send_len > 0) {
            std::memcpy(payload.data() + sizeof(ArchiveChunkHdr), send_data, send_len);
        }

        try {
            auto g = pool_.guard(conn_idx);
            g.socket().write_frame(MsgType::MT_ARCHIVE_CHUNK, 0,
                                   payload.data(), (u32)payload.size());
        } catch (const std::exception& e) {
            LOG_WARN("send_archive_range: write chunk " + std::to_string(chunk_id) +
                     " failed: " + e.what());
            return false;
        }

        tui_state_.bytes_sent.fetch_add(vc.raw_size);
    }

    // Signal end of this connection's archive range
    try {
        auto g = pool_.guard(conn_idx);
        g.socket().write_frame(MsgType::MT_ARCHIVE_DONE, 0, nullptr, 0);
    } catch (const std::exception& e) {
        LOG_WARN("send_archive_range: ARCHIVE_DONE failed: " + std::string(e.what()));
        return false;
    }

    return true;
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
