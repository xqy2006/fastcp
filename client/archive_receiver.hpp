#pragma once

// ============================================================
// archive_receiver.hpp -- Client-side Virtual Archive receiver
//
// Preallocates all destination files, receives archive chunks
// from multiple connections, and scatter-writes each chunk to
// the appropriate files via pwrite (MmapWriter::write_at).
// ============================================================

#include "../common/platform.hpp"
#include "../common/protocol.hpp"
#include "../common/hash.hpp"
#include "../common/file_io.hpp"
#include "session_manager.hpp"
#include <vector>
#include <string>
#include <memory>
#include <atomic>
#include <mutex>

// Per-file state during archive receive
struct ArchiveFileSlot {
    u32 file_id;
    u64 virtual_offset;
    u64 file_size;
    u64 mtime_ns;
    std::string abs_path;
    u8  expected_hash[16];

    std::unique_ptr<file_io::MmapWriter> writer;

    // How many archive chunks overlap with this file (set during init)
    u32 chunks_needed{0};
    // Note: chunks_written is tracked in ArchiveReceiver::chunks_written_[]
    // to avoid non-movable std::atomic embedded in this struct.
};

class ArchiveReceiver {
public:
    explicit ArchiveReceiver(std::shared_ptr<SessionInfo> session);

    // Initialise from manifest data.
    // Call after all ARCHIVE_FILE_ENTRY messages have been decoded.
    void init(std::vector<ArchiveFileSlot> slots, u32 chunk_size, u64 total_size);

    // Set up the VA resume progress file.
    // MUST be called after init() and BEFORE preallocate_all().
    // path      = absolute path to ".fastcp_va_progress" file
    // token     = 16-byte manifest token (xxh3-128 of archive file entries)
    //
    // If a valid progress file already exists (matching token), loads the set
    // of already-received chunks so build_needed_chunks() excludes them and
    // preallocate_all() opens files in resume mode (no truncation).
    void set_progress(const std::string& path, const u8 token[16]);

    // Preallocate all target files on disk.
    // When a valid progress file was loaded, existing files are NOT truncated.
    void preallocate_all();

    // Build the list of chunk IDs the client still needs.
    // Returns chunk IDs sorted ascending.
    // With set_progress: excludes already-received chunks.
    // Without set_progress: returns all chunks (0..total_chunks-1).
    std::vector<u32> build_needed_chunks() const;

    // Process one received archive chunk (thread-safe).
    // raw_data must be the decompressed (original) data, raw_len bytes.
    // Returns the list of file_ids that became complete during this call.
    std::vector<u32> on_archive_chunk(const ArchiveChunkHdr& hdr,
                                      const u8* raw_data);

    // Delete the progress file (called on successful completion).
    void finish_progress();

    u32 total_chunks() const { return total_chunks_; }

private:
    std::shared_ptr<SessionInfo> session_;
    std::vector<ArchiveFileSlot> slots_;  // sorted by virtual_offset

    // Per-slot write counter (atomic, index matches slots_)
    // Stored separately so ArchiveFileSlot remains move-constructible.
    std::vector<std::atomic<u32>> chunks_written_;

    u32 chunk_size_{0};
    u64 total_size_{0};
    u32 total_chunks_{0};

    // ---- VA resume state ----
    std::string     progress_path_;          // path to .fastcp_va_progress
    u8              manifest_token_[16]{};   // token identifying this transfer
    std::vector<u8> needed_bits_;            // bit i = 1 → chunk i still needed
    std::mutex      progress_mutex_;
    int             chunks_remaining_{0};    // count of set bits in needed_bits_
    int             save_counter_{0};        // batch save every 100 chunks

    // Mark chunk as received; flush progress file every 100 calls; delete
    // the progress file automatically when all chunks are done.
    void mark_chunk_done(u32 chunk_id);

    // Write current needed_bits_ state to progress_path_ (caller holds lock).
    void save_progress_locked();

    // Scatter-write raw_data at [archive_offset, archive_offset+len) to all
    // overlapping files. Appends completed file_ids to completed_out.
    void scatter_write(u64 archive_offset, const u8* data, u32 len,
                       std::vector<u32>& completed_out);
};
