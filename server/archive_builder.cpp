// ============================================================
// archive_builder.cpp -- Virtual Archive layout builder implementation
// ============================================================

#include "archive_builder.hpp"
#include "../common/file_io.hpp"
#include "../common/logger.hpp"
#include <algorithm>
#include <cstring>
#include <stdexcept>

void ArchiveBuilder::build(const std::vector<FileEntry>& entries, u32 chunk_size) {
    chunk_size_ = (chunk_size > 0) ? chunk_size : DEFAULT_CHUNK_SIZE;
    files_.clear();
    chunks_.clear();
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

std::vector<u8> ArchiveBuilder::read_chunk(u32 chunk_id) const {
    if (chunk_id >= (u32)chunks_.size()) {
        throw std::out_of_range("read_chunk: invalid chunk_id " +
                                std::to_string(chunk_id));
    }

    const VirtualChunk& vc = chunks_[chunk_id];
    std::vector<u8> buf(vc.raw_size, 0);

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
                // Clamp to buffer boundary (should not happen with correct layout)
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
