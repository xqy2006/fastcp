#pragma once

// ============================================================
// server_app.hpp -- FastCP server: persistent daemon
//   Listens on a port, serves CONCURRENT client sessions.
//   Each client connects with N parallel sockets to get files.
//
// Concurrency model:
//   accept_loop()  → accepts one socket at a time, spawns a
//                    lightweight "connector thread" per socket.
//   connector threads → read HANDSHAKE_REQ, group by session_id
//                       into PendingSession map.
//   When all N sockets for a session arrive, a full ClientSession
//   thread is spawned. Multiple ClientSession threads run
//   concurrently, each transferring files to one logical client.
// ============================================================

#include "../common/platform.hpp"
#include "../common/protocol.hpp"
#include "../common/socket.hpp"
#include "dir_scanner.hpp"
#include "connection_pool.hpp"
#include "file_sender.hpp"
#include "archive_builder.hpp"
#include "tui.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>

struct ServerConfig {
    std::string src_dir;
    std::string listen_ip;      // e.g. "0.0.0.0"
    u16         listen_port{9999};
    int         num_conns{4};   // parallel connections expected per client
    bool        use_compress{true};
    u32         chunk_size{DEFAULT_CHUNK_SIZE};
    int         max_chunks{0};  // test-only: abort after N chunks (0=unlimited)
    u8          dir_id[16]{};   // UUID for src_dir, loaded/created at startup
};

// Sync plan received from the client during negotiation
struct SyncPlanMap {
    // file_id -> (action, resume_offset)
    std::unordered_map<u32, std::pair<SyncAction, u64>> plan;
};

// A peeked (already-read) frame: used when accept_loop reads the first
// HANDSHAKE_REQ before handing the socket to a ClientSession.
struct PeekedFrame {
    FrameHeader         hdr{};
    std::vector<u8>     payload;
};

// ---- PendingSession -------------------------------------------
// Accumulates sockets for one logical client until all N arrive.
// The HANDSHAKE_ACK is sent immediately in connector_thread,
// so the client doesn't block waiting for all N connections to arrive.
struct PendingSession {
    u64                      session_id{0};
    int                      expected_conns{1};
    u16                      agreed_caps{0};
    u32                      agreed_chunk_size{DEFAULT_CHUNK_SIZE};
    std::vector<TcpSocket>   sockets;
    std::vector<PeekedFrame> peeked;
    std::chrono::steady_clock::time_point created_at;
};

// ---- ClientSession -----------------------------------------------
// Manages one logical transfer session: N accepted sockets,
// 4-phase protocol, one TUI progress bar.
class ClientSession {
public:
    ClientSession(const ServerConfig& cfg,
                  std::vector<FileEntry> entries,
                  u64 total_bytes,
                  std::vector<TcpSocket> sockets,
                  std::vector<PeekedFrame> peeked,
                  u64 session_id = 0,
                  u16 agreed_caps = 0,
                  u32 agreed_chunk_size = DEFAULT_CHUNK_SIZE);

    int run(); // blocks until transfer completes or fails

private:
    ServerConfig           cfg_;
    std::vector<FileEntry> entries_;
    u64                    total_bytes_{0};
    std::vector<PeekedFrame> peeked_frames_;

    ConnectionPool         pool_;
    u64                    session_id_{0};
    u16                    agreed_caps_{0};
    u32                    agreed_chunk_size_{DEFAULT_CHUNK_SIZE};

    TuiState               tui_state_;
    std::unique_ptr<Tui>   tui_;

    // Delta-sync: block checksums received from client, per file
    DeltaChecksumMap       delta_checksums_;
    DeltaBlockSizeMap      delta_block_sizes_;

    bool phase_handshake();
    bool do_handshake_peeked(int conn_idx);

    bool phase_file_list(SyncPlanMap& plan_out);
    bool phase_pipeline_sync();
    bool phase_transfer(const SyncPlanMap& plan);
    bool phase_transfer_archive(const SyncPlanMap& plan);
    bool phase_done();
};

// ---- ServerApp ---------------------------------------------------
// Bind/listen daemon; for each incoming client spawns a ClientSession.
// Multiple ClientSessions run concurrently (one thread each).
class ServerApp {
public:
    explicit ServerApp(ServerConfig config);
    ~ServerApp();

    // Blocks until stop() is called (or fatal error)
    int run();

    // Call from signal handler to shut down gracefully
    void stop();

private:
    ServerConfig           config_;
    TcpSocket              listen_sock_;
    std::atomic<bool>      running_{false};

    std::vector<FileEntry> entries_;        // pre-scanned, reused per session
    u64                    total_bytes_{0};
    std::mutex             entries_mutex_;

    // Set to true when the first client session starts, signalling the
    // background hasher to stop early (it competes for disk I/O with
    // read_chunk / pre_read_all once a transfer is in progress).
    std::atomic<bool>      bg_hasher_stop_{false};

    // --- Connector threads (one per accepted socket, short-lived) ---
    std::vector<std::thread> connector_threads_;
    std::mutex               connector_mutex_;

    // --- Pending sessions: grouping sockets by session_id ---
    std::unordered_map<u64, PendingSession> pending_;
    std::mutex                              pending_mutex_;

    // --- Full client session threads (long-lived, one per client) ---
    std::vector<std::thread> session_threads_;
    std::mutex               session_threads_mutex_;

    // Accept loop: pure accept() + spawn connector_thread per socket
    void accept_loop();

    // Connector thread: read handshake, group by session_id,
    // fire ClientSession when all N connections arrive.
    void connector_thread(TcpSocket sock);

    // Launch a ClientSession thread
    void launch_session(PendingSession ps);

    void handle_client(std::vector<FileEntry> entries,
                       u64 total_bytes,
                       std::vector<TcpSocket> sockets,
                       std::vector<PeekedFrame> peeked,
                       u64 session_id,
                       u16 agreed_caps,
                       u32 agreed_chunk_size);

    // Reap finished threads (detach)
    void cleanup_threads();

    // Generate unique session id for a new logical session
    u64 generate_session_id();

    std::atomic<u64> session_id_counter_{1};
};
