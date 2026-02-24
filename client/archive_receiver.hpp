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

    // Preallocate all target files on disk (create + truncate to required size).
    void preallocate_all();

    // Build the list of chunk IDs the client needs (all of them in initial version).
    // Returns chunk IDs sorted ascending.
    std::vector<u32> build_needed_chunks() const;

    // Process one received archive chunk (thread-safe).
    // raw_data must be the decompressed (original) data, raw_len bytes.
    // Returns the list of file_ids that became complete during this call.
    std::vector<u32> on_archive_chunk(const ArchiveChunkHdr& hdr,
                                      const u8* raw_data);

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

    // Scatter-write raw_data at [archive_offset, archive_offset+len) to all
    // overlapping files. Appends completed file_ids to completed_out.
    void scatter_write(u64 archive_offset, const u8* data, u32 len,
                       std::vector<u32>& completed_out);
};
