#pragma once

// ============================================================
// file_sender.hpp -- Server-side parallel file send logic
//
// Parallelism model:
//   - Large files can be sent using multiple connections in parallel
//   - Each connection handles a subset of chunks (stride = num_connections)
//   - Multiple files can be sent concurrently when connections available
// ============================================================

#include "../common/platform.hpp"
#include "../common/protocol.hpp"
#include "../common/socket.hpp"
#include "dir_scanner.hpp"
#include "connection_pool.hpp"
#include "tui.hpp"
#include <vector>
#include <unordered_map>
#include <string>
#include <memory>
#include <mutex>
#include <atomic>
#include <thread>
#include <functional>
#include <condition_variable>

// Delta checksums received from client
using DeltaChecksumMap  = std::unordered_map<u32, std::vector<BlockChecksumEntry>>;
using DeltaBlockSizeMap = std::unordered_map<u32, u32>;

// Static empty maps used as defaults
inline const DeltaChecksumMap&  empty_delta_checksums()  { static DeltaChecksumMap  m; return m; }
inline const DeltaBlockSizeMap& empty_delta_block_sizes(){ static DeltaBlockSizeMap m; return m; }

// Retry state for NACK handling
struct ChunkRetry {
    u32 file_id;
    u32 chunk_index;
    int retries{0};
};

// Progress callback for file completion
using FileDoneCallback = std::function<void(u32 file_id)>;

class FileSender {
public:
    FileSender(ConnectionPool& pool,
               TuiState& tui_state,
               bool use_compress,
               u32  chunk_size,
               const DeltaChecksumMap&  delta_checksums  = empty_delta_checksums(),
               const DeltaBlockSizeMap& delta_block_sizes = empty_delta_block_sizes());

    // ---- Single-threaded API (backward compatible) ----

    // Send a large file on a specific connection (legacy, single-threaded)
    bool send_large_file(const FileEntry& fe, u64 resume_offset, int conn_idx = -1);

    // Bundle multiple small files and send on one connection
    bool send_bundle(const std::vector<const FileEntry*>& files, int conn_idx = 0);

    // ---- Parallel API ----

    // Send a large file using multiple connections in parallel.
    // Each connection handles chunks where (chunk_index % num_conns) == conn_offset.
    // Call this from multiple threads with different conn_offset values.
    //
    // @param fe: file entry
    // @param resume_offset: already received bytes
    // @param conn_indices: list of connection indices to use (e.g., [0, 2, 3])
    // @param num_threads_for_file: how many parallel threads for this file
    // @param thread_idx: which thread (0..num_threads_for_file-1)
    // @param on_done: callback when file is fully sent (called by last thread)
    //
    // Thread safety: multiple threads can call this for the same file with different thread_idx.
    bool send_large_file_parallel(
        const FileEntry& fe,
        u64 resume_offset,
        const std::vector<int>& conn_indices,
        int num_threads_for_file,
        int thread_idx,
        FileDoneCallback on_done = nullptr);

    // Handle an ACK/NACK received on a connection
    bool on_ack(const Ack& ack);
    bool on_nack(const NackMsg& nack, const FileEntry* fe);

    bool flush_acks();

    // Initialize per-file parallel tracking.
    // MUST be called from the spawning thread BEFORE any worker threads start.
    void init_file_tracking(u32 file_id, int num_threads);

private:
    ConnectionPool& pool_;
    TuiState&       tui_state_;
    bool            use_compress_;
    u32             chunk_size_;
    const DeltaChecksumMap&  delta_checksums_;
    const DeltaBlockSizeMap& delta_block_sizes_;

    std::mutex retry_mutex_;
    std::vector<ChunkRetry> pending_retries_;
    std::atomic<int> nack_errors_{0};

    // For parallel file sending: track completion status
    std::mutex file_completion_mutex_;
    std::unordered_map<u32, int>  file_chunks_pending_;   // file_id -> remaining chunks  (protected by file_completion_mutex_)
    std::unordered_map<u32, int>  file_threads_pending_;  // file_id -> remaining threads (protected by file_completion_mutex_)
    std::unordered_map<u32, bool> file_success_;          // file_id -> success status     (protected by file_completion_mutex_)

    // Synchronization: wait for FileMeta before sending chunks
    std::mutex file_meta_mutex_;
    std::condition_variable file_meta_cv_;
    std::unordered_map<u32, bool> file_meta_ready_;

    bool send_chunk(TcpSocket& sock,
                    const FileEntry& fe,
                    u32 chunk_index,
                    u64 offset,
                    const char* data,
                    u32 raw_len,
                    bool compress);

    // Check if a block matches the client's checksums (can skip)
    bool block_matches_client(u32 file_id, u32 block_index,
                              const char* data, u32 len) const;
};
