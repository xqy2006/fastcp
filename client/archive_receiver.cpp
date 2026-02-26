// ============================================================
// archive_receiver.cpp -- Client-side Virtual Archive receiver
// ============================================================

#include "archive_receiver.hpp"
#include "../common/compress.hpp"
#include "../common/hash.hpp"
#include "../common/logger.hpp"
#include "../common/utils.hpp"
#ifndef _WIN32
#  include <fcntl.h>
#  include <sys/stat.h>
#endif
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <stdexcept>
#include <thread>
#include <unordered_set>

namespace fs = std::filesystem;

ArchiveReceiver::ArchiveReceiver(std::shared_ptr<SessionInfo> session)
    : session_(std::move(session))
{}

ArchiveReceiver::~ArchiveReceiver() {
    // Save progress on destruction so interrupts don't lose received chunks
    std::lock_guard<std::mutex> lk(progress_mutex_);
    save_progress_locked();
}

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
    // If we loaded a valid progress file, some chunks are already written to
    // disk — open existing files without truncation (resume=true).
    bool resuming = !progress_path_.empty() &&
                    chunks_remaining_ < (int)total_chunks_;

    // Cache already-created parent directories to avoid redundant
    // fs::create_directories() calls for files in the same directory.
    std::unordered_set<std::string> created_dirs;

    for (auto& slot : slots_) {
        try {
            // Only call ensure_parent_dirs once per unique parent directory.
            std::string parent = fs::path(slot.abs_path).parent_path().string();
            if (created_dirs.find(parent) == created_dirs.end()) {
                file_io::ensure_parent_dirs(slot.abs_path);
                created_dirs.insert(std::move(parent));
            }

            if (slot.file_size == 0) {
                // Empty file: create immediately
                file_io::MmapWriter w;
                w.open(slot.abs_path, 0, false);
                w.close(slot.mtime_ns);
                session_->files_done.fetch_add(1);
            } else if (slot.chunks_needed <= 1) {
                // Single-chunk small file: skip pre-creation entirely.
                // scatter_write() will open with O_CREAT|O_TRUNC (CREATE_ALWAYS
                // on Windows), write data, and set mtime all in one handle —
                // reducing 3 open/close pairs to 1.
                // slot.writer remains nullptr  →  merged write mode in scatter_write
                (void)resuming; // no pre-creation needed regardless of resume state
            } else {
                // Multi-chunk large file: use MmapWriter for random-access writes
                slot.writer = std::make_unique<file_io::MmapWriter>();
                slot.writer->open(slot.abs_path, slot.file_size, resuming);
            }
        } catch (const std::exception& e) {
            LOG_WARN("archive_receiver: preallocate failed for " +
                     slot.abs_path + ": " + e.what());
        }
    }
}

std::vector<u32> ArchiveReceiver::build_needed_chunks() const {
    std::vector<u32> ids;
    if (needed_bits_.empty()) {
        // No progress file loaded — need every chunk (fresh transfer)
        ids.reserve(total_chunks_);
        for (u32 i = 0; i < total_chunks_; ++i) ids.push_back(i);
    } else {
        // Progress file loaded — only return chunks whose bit is still set
        ids.reserve((size_t)chunks_remaining_);
        for (u32 i = 0; i < total_chunks_; ++i) {
            if (needed_bits_[i / 8] & (u8)(1u << (i % 8)))
                ids.push_back(i);
        }
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
    mark_chunk_done(hdr.chunk_id);
    return completed;
}

// ---- VA resume: progress file ----------------------------------------

void ArchiveReceiver::set_progress(const std::string& path, const u8 token[16]) {
    if (total_chunks_ == 0) return;

    progress_path_ = path;
    std::memcpy(manifest_token_, token, 16);

    // Initialise bitset: all chunks needed (bit = 1)
    u32 bytes = (total_chunks_ + 7) / 8;
    needed_bits_.assign(bytes, 0xFF);
    // Clear trailing bits beyond total_chunks_
    if (total_chunks_ % 8 != 0) {
        needed_bits_.back() = (u8)((1u << (total_chunks_ % 8)) - 1);
    }
    chunks_remaining_ = (int)total_chunks_;

    // Try to load an existing progress file
    std::ifstream f(path, std::ios::binary);
    if (!f) { LOG_INFO("VA resume: no progress file at " + path); return; }

    u8 saved_token[16]{};
    f.read((char*)saved_token, 16);
    if (!f || std::memcmp(saved_token, token, 16) != 0) {
        LOG_INFO("VA resume: token mismatch, ignoring progress file");
        return;
    }

    u32 saved_total = 0;
    f.read((char*)&saved_total, 4);
    if (!f || saved_total != total_chunks_) {
        LOG_INFO("VA resume: chunk count mismatch (saved=" + std::to_string(saved_total) +
                 " current=" + std::to_string(total_chunks_) + "), ignoring");
        return;
    }

    std::vector<u8> saved_bits(bytes);
    f.read((char*)saved_bits.data(), (std::streamsize)bytes);
    if ((size_t)f.gcount() != bytes) { LOG_INFO("VA resume: truncated progress file"); return; }

    // Count remaining needed chunks from saved state
    needed_bits_ = saved_bits;
    chunks_remaining_ = 0;
    u32 already_done = 0;
    for (u32 i = 0; i < total_chunks_; ++i) {
        if (needed_bits_[i / 8] & (u8)(1u << (i % 8)))
            ++chunks_remaining_;
        else
            ++already_done;
    }
    // Seed bytes_received with already-completed bytes so TUI shows correct progress
    u64 done_bytes = (u64)already_done * chunk_size_;
    // Last chunk may be smaller; cap at total_size_
    if (done_bytes > total_size_) done_bytes = total_size_;
    session_->bytes_received.fetch_add(done_bytes);

    // Seed files_done: count files whose every chunk is already received
    for (size_t si = 0; si < slots_.size(); ++si) {
        const auto& slot = slots_[si];
        if (slot.file_size == 0) { session_->files_done.fetch_add(1); continue; }
        u32 first_chunk = (u32)(slot.virtual_offset / chunk_size_);
        u32 last_chunk  = (u32)((slot.virtual_offset + slot.file_size - 1) / chunk_size_);
        u32 done_count = 0;
        bool all_done = true;
        for (u32 ci = first_chunk; ci <= last_chunk && ci < total_chunks_; ++ci) {
            if (needed_bits_[ci / 8] & (u8)(1u << (ci % 8))) {
                all_done = false;
            } else {
                ++done_count;
            }
        }
        // Seed chunks_written_ so completion check works correctly on resume
        if (done_count > 0)
            chunks_written_[si].store(done_count);
        if (all_done) session_->files_done.fetch_add(1);
    }

    LOG_INFO("VA resume: progress loaded — " +
             std::to_string(total_chunks_ - chunks_remaining_) + "/" +
             std::to_string(total_chunks_) + " chunks already received");
}

void ArchiveReceiver::mark_chunk_done(u32 chunk_id) {
    if (progress_path_.empty() || chunk_id >= total_chunks_) return;

    std::lock_guard<std::mutex> lk(progress_mutex_);
    u32 byte_idx = chunk_id / 8;
    u8  bit_mask = (u8)(1u << (chunk_id % 8));

    if (!(needed_bits_[byte_idx] & bit_mask)) return; // already marked

    needed_bits_[byte_idx] &= ~bit_mask;
    if (--chunks_remaining_ == 0) {
        // All chunks received — delete progress file
        std::error_code ec;
        fs::remove(fs::path(progress_path_), ec);
        progress_path_.clear();
        return;
    }
    save_progress_locked();}

void ArchiveReceiver::save_progress_locked() {
    // Called while holding progress_mutex_
    if (progress_path_.empty()) return;
    std::ofstream f(progress_path_, std::ios::binary | std::ios::trunc);
    if (!f) return;
    f.write((char*)manifest_token_, 16);
    u32 tc = total_chunks_;
    f.write((char*)&tc, 4);
    f.write((char*)needed_bits_.data(), (std::streamsize)needed_bits_.size());
}

void ArchiveReceiver::finish_progress() {
    std::lock_guard<std::mutex> lk(progress_mutex_);
    if (progress_path_.empty()) return;
    // Save final state (useful if some chunks had hash errors and weren't marked)
    save_progress_locked();
    // Only delete if truly nothing remains
    if (chunks_remaining_ == 0) {
        std::error_code ec;
        fs::remove(fs::path(progress_path_), ec);
        progress_path_.clear();
    }
}

void ArchiveReceiver::scatter_write(u64 archive_offset,
                                     const u8* data,
                                     u32 len,
                                     std::vector<u32>& completed_out)
{
    if (len == 0 || !data) return;

    u64 chunk_end = archive_offset + len;

    // ---- Pass 1: collect all overlapping slots (binary-search entry point) ----
    // slots_ is sorted by virtual_offset; find the first slot that could overlap.
    struct WorkItem {
        size_t si;
        u64 chunk_rel;  // offset into chunk data where this file's overlap starts
        u64 file_off;   // offset within the file
        u32 write_len;
    };
    std::vector<WorkItem> work;

    // Find the first slot that could overlap: virtual_offset + file_size > archive_offset
    // Use linear scan from the beginning for now; slots_ already sorted so we break early.
    for (size_t si = 0; si < slots_.size(); ++si) {
        auto& slot = slots_[si];
        if (slot.file_size == 0) continue;
        u64 file_start = slot.virtual_offset;
        u64 file_end   = file_start + slot.file_size;
        if (file_start >= chunk_end) break;
        if (file_end   <= archive_offset) continue;

        u64 overlap_start = std::max(archive_offset, file_start);
        u64 overlap_end   = std::min(chunk_end,      file_end);
        if (overlap_start >= overlap_end) continue;

        work.push_back({si,
                        overlap_start - archive_offset,
                        overlap_start - file_start,
                        (u32)(overlap_end - overlap_start)});
    }

    if (work.empty()) return;

    // ---- Pass 2: write each slot, parallel when many small files in one chunk ----
    // Threshold: parallelise when ≥8 slots AND each write is small (≤BUNDLE_THRESHOLD).
    // For large multi-chunk files, a single slot owns the whole chunk → serial is fine.
    const size_t PAR_THRESHOLD = 8;
    const int    NTHREADS      = 8;

    // completed_ids[t] collects file_ids completed by thread t
    std::vector<std::vector<u32>> completed_ids((size_t)NTHREADS);

    // process_one: write one WorkItem and handle completion check.
    // Returns file_id if this call completed the file, else 0.
    auto process_one = [&](const WorkItem& wi) -> u32 {
        auto& slot     = slots_[wi.si];
        bool  write_ok = false;

        if (slot.writer && slot.writer->is_open()) {
            slot.writer->write_at(wi.file_off, data + wi.chunk_rel, wi.write_len);
            session_->bytes_received.fetch_add(wi.write_len);
            write_ok = true;
        } else if (!slot.writer) {
            // Merged write: single-chunk small file
#ifndef _WIN32
            int fd = ::open(slot.abs_path.c_str(),
                            O_CREAT | O_WRONLY | O_TRUNC, 0644);
            if (fd >= 0) {
                ssize_t nw = ::pwrite(fd, data + wi.chunk_rel, wi.write_len,
                                      (off_t)wi.file_off);
                write_ok = (nw == (ssize_t)wi.write_len);
                struct timespec ts[2];
                ts[0].tv_sec  = (time_t)(slot.mtime_ns / 1000000000ULL);
                ts[0].tv_nsec = (long)(slot.mtime_ns % 1000000000ULL);
                ts[1] = ts[0];
                ::futimens(fd, ts);
                ::close(fd);
                if (write_ok) session_->bytes_received.fetch_add(wi.write_len);
            } else {
                LOG_WARN("archive_receiver: create/open failed for " + slot.abs_path);
            }
#else
            u64 ft_val = slot.mtime_ns / 100ULL + 116444736000000000ULL;
            FILETIME ft;
            ft.dwLowDateTime  = (DWORD)(ft_val & 0xFFFFFFFFUL);
            ft.dwHighDateTime = (DWORD)(ft_val >> 32);
            HANDLE h = CreateFileA(slot.abs_path.c_str(),
                GENERIC_WRITE | FILE_WRITE_ATTRIBUTES, 0, nullptr,
                CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (h != INVALID_HANDLE_VALUE) {
                LARGE_INTEGER li; li.QuadPart = (LONGLONG)wi.file_off;
                SetFilePointerEx(h, li, nullptr, FILE_BEGIN);
                DWORD nw = 0;
                WriteFile(h, data + wi.chunk_rel, (DWORD)wi.write_len, &nw, nullptr);
                write_ok = (nw == (DWORD)wi.write_len);
                SetFileTime(h, nullptr, nullptr, &ft);
                CloseHandle(h);
                if (write_ok) session_->bytes_received.fetch_add(wi.write_len);
            } else {
                LOG_WARN("archive_receiver: create failed for " + slot.abs_path);
            }
#endif
        }

        // Completion check
        u32 written = chunks_written_[wi.si].fetch_add(1) + 1;
        if (written < slot.chunks_needed || slot.chunks_needed == 0)
            return 0;

        if (slot.writer && slot.writer->is_open())
            slot.writer->close(slot.mtime_ns);

        bool hash_ok = true;
        if (!slot.writer && slot.chunks_needed <= 1) {
            if (!write_ok) {
                hash_ok = false;
                LOG_WARN("archive_receiver: write failed for " + slot.abs_path);
            } else {
                auto digest = hash::xxh3_128(data + wi.chunk_rel, wi.write_len);
                hash_ok = (std::memcmp(digest.data(), slot.expected_hash, 16) == 0);
                if (!hash_ok)
                    LOG_WARN("archive_receiver: hash mismatch for " + slot.abs_path);
            }
        } else {
            try {
                file_io::MmapReader verify(slot.abs_path);
                auto digest = hash::xxh3_128(verify.data(), verify.size());
                hash_ok = (std::memcmp(digest.data(), slot.expected_hash, 16) == 0);
                if (!hash_ok)
                    LOG_WARN("archive_receiver: hash mismatch for " + slot.abs_path);
            } catch (const std::exception& e) {
                LOG_WARN("archive_receiver: verify read failed for " +
                         slot.abs_path + ": " + e.what());
                hash_ok = false;
            }
        }

        if (hash_ok) {
            session_->files_done.fetch_add(1);
            LOG_DEBUG("archive_receiver: file complete: " + slot.abs_path);
            return slot.file_id;
        }

        // Hash mismatch: delete the corrupted file and re-mark all its chunks
        // as needed so the next resume will re-request and re-receive them.
        {
            std::error_code ec;
            fs::remove(fs::path(slot.abs_path), ec);
        }
        chunks_written_[wi.si].store(0);
        if (!progress_path_.empty() && slot.file_size > 0) {
            u32 first_chunk = (u32)(slot.virtual_offset / chunk_size_);
            u32 last_chunk  = (u32)((slot.virtual_offset + slot.file_size - 1) / chunk_size_);
            std::lock_guard<std::mutex> lk(progress_mutex_);
            for (u32 ci = first_chunk; ci <= last_chunk && ci < total_chunks_; ++ci) {
                u8 bit = (u8)(1u << (ci % 8));
                if (!(needed_bits_[ci / 8] & bit)) {
                    needed_bits_[ci / 8] |= bit;
                    ++chunks_remaining_;
                }
            }
            save_progress_locked();
        }
        return 0;
    };

    if (work.size() >= PAR_THRESHOLD) {
        // Parallel: split work round-robin across NTHREADS threads
        std::vector<std::thread> workers;
        workers.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; ++t) {
            workers.emplace_back([&, t]() {
                auto& out = completed_ids[(size_t)t];
                for (size_t i = (size_t)t; i < work.size(); i += (size_t)NTHREADS) {
                    u32 fid = process_one(work[i]);
                    if (fid) out.push_back(fid);
                }
            });
        }
        for (auto& w : workers) w.join();
        for (auto& out : completed_ids)
            for (u32 fid : out) completed_out.push_back(fid);
    } else {
        // Serial: no threading overhead for small overlap counts
        for (const auto& wi : work) {
            u32 fid = process_one(wi);
            if (fid) completed_out.push_back(fid);
        }
    }
}
