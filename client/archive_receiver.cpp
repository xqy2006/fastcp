// ============================================================
// archive_receiver.cpp -- Client-side Virtual Archive receiver
// ============================================================

#include "archive_receiver.hpp"
#include "../common/compress.hpp"
#include "../common/hash.hpp"
#include "../common/logger.hpp"
#include "../common/utils.hpp"
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <numeric>
#include <stdexcept>

namespace fs = std::filesystem;

ArchiveReceiver::ArchiveReceiver(std::shared_ptr<SessionInfo> session)
    : session_(std::move(session))
{}

void ArchiveReceiver::init(std::vector<ArchiveFileSlot> slots,
                            u32 chunk_size,
                            u64 total_size)
{
    chunk_size_   = (chunk_size > 0) ? chunk_size : DEFAULT_CHUNK_SIZE;
    total_size_   = total_size;
    total_chunks_ = (total_size_ > 0)
                    ? (u32)((total_size_ + chunk_size_ - 1) / chunk_size_)
                    : 0u;

    // Sort slots by virtual_offset using an index array (slots contain
    // unique_ptr so they can only be moved, not copied; sort by index first)
    std::vector<size_t> order(slots.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
        return slots[a].virtual_offset < slots[b].virtual_offset;
    });

    slots_.reserve(slots.size());
    for (size_t i : order) {
        slots_.push_back(std::move(slots[i]));
    }

    // Compute chunks_needed for each file
    for (auto& slot : slots_) {
        if (slot.file_size == 0) {
            slot.chunks_needed = 0;
            continue;
        }
        u64 first = slot.virtual_offset / chunk_size_;
        u64 last  = (slot.virtual_offset + slot.file_size - 1) / chunk_size_;
        slot.chunks_needed = (u32)(last - first + 1);
    }

    // Initialize per-slot write counters (atomic<u32> default-constructed = 0)
    chunks_written_ = std::vector<std::atomic<u32>>(slots_.size());
}

void ArchiveReceiver::preallocate_all() {
    for (auto& slot : slots_) {
        try {
            file_io::ensure_parent_dirs(slot.abs_path);

            if (slot.file_size > 0) {
                slot.writer = std::make_unique<file_io::MmapWriter>();
                slot.writer->open(slot.abs_path, slot.file_size, false);
            } else {
                // Empty file: create immediately
                file_io::MmapWriter w;
                w.open(slot.abs_path, 0, false);
                w.close(slot.mtime_ns);
                session_->files_done.fetch_add(1);
            }
        } catch (const std::exception& e) {
            LOG_WARN("archive_receiver: preallocate failed for " +
                     slot.abs_path + ": " + e.what());
        }
    }
}

std::vector<u32> ArchiveReceiver::build_needed_chunks() const {
    std::vector<u32> ids;
    ids.reserve(total_chunks_);
    for (u32 i = 0; i < total_chunks_; ++i) {
        ids.push_back(i);
    }
    return ids;
}

std::vector<u32> ArchiveReceiver::on_archive_chunk(const ArchiveChunkHdr& hdr,
                                                     const u8* raw_data)
{
    std::vector<u32> completed;

    // Validate hash of raw data
    if (hdr.raw_len > 0 && raw_data) {
        u32 computed = hash::xxh3_32(raw_data, hdr.raw_len);
        if (computed != hdr.xxh3_32) {
            LOG_WARN("archive_receiver: hash mismatch on chunk " +
                     std::to_string(hdr.chunk_id) +
                     " (expected=" + std::to_string(hdr.xxh3_32) +
                     " got=" + std::to_string(computed) + ")");
            return completed;
        }
    }

    scatter_write(hdr.archive_offset, raw_data, hdr.raw_len, completed);
    return completed;
}

void ArchiveReceiver::scatter_write(u64 archive_offset,
                                     const u8* data,
                                     u32 len,
                                     std::vector<u32>& completed_out)
{
    if (len == 0 || !data) return;

    u64 chunk_end = archive_offset + len;

    for (size_t si = 0; si < slots_.size(); ++si) {
        auto& slot = slots_[si];
        if (slot.file_size == 0) continue;

        u64 file_start = slot.virtual_offset;
        u64 file_end   = slot.virtual_offset + slot.file_size;

        // slots_ is sorted by virtual_offset; once we're past the chunk, stop
        if (file_start >= chunk_end) break;
        if (file_end   <= archive_offset) continue;

        // Compute overlap
        u64 overlap_start = std::max(archive_offset, file_start);
        u64 overlap_end   = std::min(chunk_end,      file_end);
        if (overlap_start >= overlap_end) continue;

        u64 chunk_rel = overlap_start - archive_offset; // offset into chunk data
        u64 file_off  = overlap_start - file_start;     // offset within file
        u32 write_len = (u32)(overlap_end - overlap_start);

        if (slot.writer && slot.writer->is_open()) {
            slot.writer->write_at(file_off, data + chunk_rel, write_len);
            session_->bytes_received.fetch_add(write_len);
        }

        // Track completion atomically (multiple threads may write to same file)
        u32 written = chunks_written_[si].fetch_add(1) + 1;
        if (written >= slot.chunks_needed && slot.chunks_needed > 0) {
            // All chunks for this file written - flush and verify
            if (slot.writer && slot.writer->is_open()) {
                slot.writer->close(slot.mtime_ns);
            }

            // Hash verification
            bool hash_ok = true;
            try {
                file_io::MmapReader verify(slot.abs_path);
                auto digest = hash::xxh3_128(verify.data(), verify.size());
                hash_ok = (std::memcmp(digest.data(), slot.expected_hash, 16) == 0);
                if (!hash_ok) {
                    LOG_WARN("archive_receiver: hash mismatch for " + slot.abs_path);
                }
            } catch (const std::exception& e) {
                LOG_WARN("archive_receiver: verify read failed for " +
                         slot.abs_path + ": " + e.what());
                hash_ok = false;
            }

            if (hash_ok) {
                session_->files_done.fetch_add(1);
                completed_out.push_back(slot.file_id);
                LOG_DEBUG("archive_receiver: file complete: " + slot.abs_path);
            }
        }
    }
}
