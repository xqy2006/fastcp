#pragma once

// ============================================================
// file_receiver.hpp -- Server-side file receive logic
// ============================================================

#include "../common/platform.hpp"
#include "../common/protocol.hpp"
#include "../common/file_io.hpp"
#include "../common/hash.hpp"
#include "../common/logger.hpp"
#include "session_manager.hpp"
#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>

// State for one file being received
struct FileReceiveState {
    u32 file_id;
    u64 file_size;
    u64 mtime_ns;
    u8  expected_hash[16];
    u32 chunk_count;
    u32 chunk_size;
    u8  compress_algo;
    std::string abs_path;
    std::string rel_path;

    file_io::MmapWriter writer;
    std::atomic<u32> chunks_received{0};
    std::atomic<u64> bytes_received{0};

    // For hash verification
    hash::StreamHasher128 hasher;
    std::mutex hasher_mutex; // chunks may arrive on different connections

    bool complete{false};
};

class FileReceiver {
public:
    explicit FileReceiver(std::shared_ptr<SessionInfo> session)
        : session_(std::move(session)) {}

    // Handle FILE_META: open file, set up state
    // Returns true on success, false on error
    bool on_file_meta(const FileMeta& meta, const std::string& rel_path);

    // Handle FILE_CHUNK: write chunk data
    // Returns NACK chunk_index if hash mismatch, -1 if internal error, 0 if ok
    int on_file_chunk(const FileChunk& chunk, const u8* data);

    // Handle FILE_END: verify full-file hash, finalize
    // Returns true if verified OK
    bool on_file_end(const FileEnd& end_msg);

    // Handle BUNDLE (small files in one message)
    bool on_bundle_entry(const BundleEntryHdr& hdr,
                          const std::string& rel_path,
                          const u8* data, u64 data_len);

    // Chunk-level resume: compare server's per-chunk hashes against local partial file.
    // Returns the list of chunk indices the client still needs (missing or hash mismatch).
    // Called after on_file_meta so FileReceiveState (with abs_path) is already set up.
    std::vector<u32> get_needed_chunks(u32 file_id,
                                        u32 chunk_count,
                                        u32 chunk_size,
                                        const u32* server_hashes);

    // Lookup active receive state
    FileReceiveState* get_state(u32 file_id);

private:
    std::shared_ptr<SessionInfo> session_;
    std::unordered_map<u32, std::unique_ptr<FileReceiveState>> states_;
    std::mutex states_mutex_;
};
