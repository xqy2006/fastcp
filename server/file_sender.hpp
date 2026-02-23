#pragma once

// ============================================================
// file_sender.hpp -- Server-side file send logic
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

// Retry state for NACK handling
struct ChunkRetry {
    u32 file_id;
    u32 chunk_index;
    int retries{0};
};

// Delta checksums received from client
using DeltaChecksumMap  = std::unordered_map<u32, std::vector<BlockChecksumEntry>>;
using DeltaBlockSizeMap = std::unordered_map<u32, u32>;

// Static empty maps used as defaults
inline const DeltaChecksumMap&  empty_delta_checksums()  { static DeltaChecksumMap  m; return m; }
inline const DeltaBlockSizeMap& empty_delta_block_sizes(){ static DeltaBlockSizeMap m; return m; }

class FileSender {
public:
    FileSender(ConnectionPool& pool,
               TuiState& tui_state,
               bool use_compress,
               u32  chunk_size,
               const DeltaChecksumMap&  delta_checksums  = empty_delta_checksums(),
               const DeltaBlockSizeMap& delta_block_sizes = empty_delta_block_sizes());

    // Send a large file (> BUNDLE_THRESHOLD)
    // resume_offset: already received by client
    bool send_large_file(const FileEntry& fe, u64 resume_offset, int conn_idx = -1);

    // Bundle multiple small files and send on one connection
    bool send_bundle(const std::vector<const FileEntry*>& files, int conn_idx = 0);

    // Handle an ACK/NACK received on a connection
    bool on_ack(const Ack& ack);
    bool on_nack(const NackMsg& nack, const FileEntry* fe);

    bool flush_acks();

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
