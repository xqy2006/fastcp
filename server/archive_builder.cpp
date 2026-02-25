// ============================================================
// archive_builder.cpp -- Virtual Archive layout builder implementation
// ============================================================

#include "archive_builder.hpp"
#include "../common/file_io.hpp"
#include "../common/logger.hpp"
#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <vector>

void ArchiveBuilder::build(const std::vector<FileEntry>& entries, u32 chunk_size) {
    chunk_size_ = (chunk_size > 0) ? chunk_size : DEFAULT_CHUNK_SIZE;
    files_.clear();
    chunks_.clear();
    pre_read_buf_.clear();
    total_size_ = 0;

    // 1. Assign virtual offsets to each file in order
    for (const auto& fe : entries) {
        VirtualFile vf;
        vf.file_id        = fe.file_id;
        vf.virtual_offset = total_size_;
        vf.file_size      = fe.file_size;
        vf.mtime_ns       = fe.mtime_ns;
        vf.xxh3_128       = fe.xxh3_128;
        vf.rel_path       = fe.rel_path;
        vf.abs_path       = fe.abs_path;
        files_.push_back(std::move(vf));
        total_size_ += fe.file_size;
    }

    if (total_size_ == 0) {
        // Nothing to archive
        return;
    }

    // 2. Divide the virtual stream into fixed-size chunks
    u32 total_chunks = (u32)((total_size_ + chunk_size_ - 1) / chunk_size_);

    for (u32 ci = 0; ci < total_chunks; ++ci) {
        u64 chunk_start = (u64)ci * chunk_size_;
        u64 chunk_end   = std::min(chunk_start + chunk_size_, total_size_);
        u32 raw_size    = (u32)(chunk_end - chunk_start);

        VirtualChunk vc;
        vc.chunk_id       = ci;
        vc.archive_offset = chunk_start;
        vc.raw_size       = raw_size;

        // 3. Find all files that overlap with [chunk_start, chunk_end)
        for (u32 fi = 0; fi < (u32)files_.size(); ++fi) {
            const VirtualFile& vf = files_[fi];
            if (vf.file_size == 0) continue;

            u64 file_start = vf.virtual_offset;
            u64 file_end   = vf.virtual_offset + vf.file_size;

            // Overlap interval
            u64 overlap_start = std::max(chunk_start, file_start);
            u64 overlap_end   = std::min(chunk_end,   file_end);

            if (overlap_start >= overlap_end) continue;

            ChunkSpan span;
            span.file_idx    = fi;
            span.file_offset = overlap_start - file_start;
            span.length      = (u32)(overlap_end - overlap_start);
            vc.spans.push_back(span);
        }

        chunks_.push_back(std::move(vc));
    }
}

bool ArchiveBuilder::pre_read_all(u64 max_bytes) {
    if (total_size_ == 0) return true;
    if (total_size_ > max_bytes) return false;

    pre_read_buf_.assign((size_t)total_size_, 0);

    // Parallel read: split files across N worker threads.
    // On Linux overlayfs each open() costs ~400 µs; 8 threads reduce
    // 10 000 files from ~4 s serial to ~0.5 s parallel.
    // On Windows NTFS the gain is smaller but still meaningful.
    const int NTHREADS = 8;
    size_t n = files_.size();
    std::vector<std::thread> workers;
    workers.reserve(NTHREADS);

    for (int t = 0; t < NTHREADS; ++t) {
        workers.emplace_back([&, t]() {
            for (size_t i = (size_t)t; i < n; i += (size_t)NTHREADS) {
                auto& vf = files_[i];
                if (vf.file_size == 0) {
                    if (hash::is_zero(vf.xxh3_128))
                        vf.xxh3_128 = hash::xxh3_128(nullptr, 0);
                    continue;
                }

                u8* dst = pre_read_buf_.data() + vf.virtual_offset;
                bool ok = false;

#ifndef _WIN32
                // O_NOATIME: skip atime update on overlayfs (saves a metadata write per file).
                // pread: no implicit lseek, safe for concurrent threads sharing no fd state.
                int fd = ::open(vf.abs_path.c_str(), O_RDONLY | O_NOATIME);
                if (fd < 0) fd = ::open(vf.abs_path.c_str(), O_RDONLY); // fallback if EPERM
                if (fd >= 0) {
                    ssize_t nr = ::pread(fd, dst, (size_t)vf.file_size, 0);
                    ::close(fd);
                    ok = (nr == (ssize_t)vf.file_size);
                }
#else
                HANDLE h = CreateFileA(vf.abs_path.c_str(),
                                       GENERIC_READ, FILE_SHARE_READ, nullptr,
                                       OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
                if (h != INVALID_HANDLE_VALUE) {
                    DWORD nr = 0;
                    ReadFile(h, dst, (DWORD)vf.file_size, &nr, nullptr);
                    CloseHandle(h);
                    ok = (nr == (DWORD)vf.file_size);
                }
#endif
                if (ok) {
                    vf.xxh3_128 = hash::xxh3_128(dst, (size_t)vf.file_size);
                } else {
                    LOG_WARN("archive_builder: pre_read_all: cannot read " + vf.abs_path);
                    std::memset(dst, 0, (size_t)vf.file_size);
                }
            }
        });
    }
    for (auto& w : workers) w.join();

    return true;
}

std::vector<u8> ArchiveBuilder::read_chunk(u32 chunk_id) const {
    if (chunk_id >= (u32)chunks_.size()) {
        throw std::out_of_range("read_chunk: invalid chunk_id " +
                                std::to_string(chunk_id));
    }

    const VirtualChunk& vc = chunks_[chunk_id];
    std::vector<u8> buf(vc.raw_size, 0);

    // Fast path: serve from the pre-read in-memory buffer (no disk I/O).
    if (!pre_read_buf_.empty()) {
        u64 chunk_start = vc.archive_offset;
        u64 available   = pre_read_buf_.size() >= chunk_start
                          ? pre_read_buf_.size() - chunk_start : 0;
        u32 copy_len    = (u32)std::min((u64)vc.raw_size, available);
        if (copy_len > 0) {
            std::memcpy(buf.data(), pre_read_buf_.data() + chunk_start, copy_len);
        }
        return buf;
    }

    // Slow path: open each source file individually (MmapReader).
    // Used when pre_read_all() was not called or the archive exceeds the
    // memory limit (> PRE_READ_MAX_BYTES).
    u64 chunk_start = vc.archive_offset;

    for (const ChunkSpan& span : vc.spans) {
        const VirtualFile& vf = files_[span.file_idx];

        // Where in the chunk buffer does this span begin?
        u64 buf_offset = (vf.virtual_offset + span.file_offset) - chunk_start;

        if (span.length == 0) continue;

        try {
            file_io::MmapReader reader(vf.abs_path);
            const char* src = reader.chunk_ptr(span.file_offset);
            if (!src) continue;

            u64 available = reader.chunk_len(span.file_offset, span.length);
            u32 copy_len  = (u32)std::min((u64)span.length, available);

            if (buf_offset + copy_len > buf.size()) {
                copy_len = (u32)(buf.size() - buf_offset);
            }

            std::memcpy(buf.data() + buf_offset, src, copy_len);
        } catch (const std::exception& e) {
            LOG_WARN("archive_builder: cannot read " + vf.abs_path + ": " + e.what());
            // Leave the span as zeros in the buffer
        }
    }

    return buf;
}
