#pragma once

// ============================================================
// session_manager.hpp -- Multi-connection session aggregation
// ============================================================

#include "../common/platform.hpp"
#include "../common/protocol.hpp"
#include "../common/transfer_index.hpp"
#include <memory>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <string>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <functional>

// Description of a file the client wants to send
struct ClientFileEntry {
    u32  file_id;
    u64  file_size;
    u64  mtime_ns;
    u8   xxh3_128[16];
    std::string rel_path; // relative path on server
    u8   flags;
};

// Represents one active session (group of N connections from the same client)
struct SessionInfo {
    u64  session_id;
    int  total_conns;          // expected number of connections
    std::atomic<int> conn_count{0}; // connected so far
    u16  capabilities;
    u32  chunk_size;

    // File list from client (populated by first connection)
    std::vector<ClientFileEntry> file_list;
    std::mutex file_list_mutex;
    bool file_list_ready{false};
    std::condition_variable file_list_cv;

    // Sync plan (server sends back to client)
    std::vector<SyncPlanEntry> sync_plan;

    std::string root_dir;
    std::unique_ptr<TransferIndex> transfer_index;

    // Delta sync: set of file_ids for which block checksums were sent to server
    // Used by FileReceiver to avoid truncating files with potentially reusable data
    std::unordered_map<u32, bool> delta_sent; // file_id -> true if checksums sent
    std::mutex delta_sent_mutex;

    // Shared FileReceiver for all connections (set by conn 0, reused by others)
    std::shared_ptr<void> shared_receiver;
    std::mutex receiver_mutex;

    // Shared ArchiveReceiver for all connections in archive mode
    std::shared_ptr<void> shared_archive_receiver;
    std::mutex archive_receiver_mutex;
    std::condition_variable archive_receiver_cv;
    std::atomic<int> archive_done_count{0};

    // Stats
    std::atomic<u64> bytes_received{0};
    std::atomic<u32> files_done{0};
    std::atomic<u32> files_total{0};

    std::chrono::steady_clock::time_point start_time;
    std::atomic<bool> done{false};

    SessionInfo() = default;
    SessionInfo(const SessionInfo&) = delete;
    SessionInfo& operator=(const SessionInfo&) = delete;
};

class SessionManager {
public:
    explicit SessionManager(const std::string& root_dir)
        : root_dir_(root_dir) {}

    // Called by first connection (session_id=0 -> server generates one)
    // Returns the session
    std::shared_ptr<SessionInfo> create_session(
        int total_conns, u16 capabilities, u32 chunk_size);

    // Called by subsequent connections with known session_id
    std::shared_ptr<SessionInfo> get_session(u64 session_id);

    // Remove completed/timed-out sessions
    void gc_sessions();

    const std::string& root_dir() const { return root_dir_; }

private:
    std::string root_dir_;
    std::mutex mutex_;
    std::unordered_map<u64, std::shared_ptr<SessionInfo>> sessions_;
};
