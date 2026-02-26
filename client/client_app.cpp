// ============================================================
// client_app.cpp -- FastCP client: connect to server, receive
// ============================================================

#include "client_app.hpp"
#include "connection_handler.hpp"
#include "../common/logger.hpp"
#include "../common/utils.hpp"
#include "../common/protocol_io.hpp"
#include "../common/file_io.hpp"
#include "../common/tui.hpp"
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <thread>
#include <chrono>
#include <cstring>
#include <iostream>

namespace fs = std::filesystem;

// ---------------------------------------------------------------
// has_tree_cache
//   Check if a previous sync left a tree cache in dst/.fastcp/.
//   If so, use Pipeline Sync (incremental) — the sync itself will
//   detect missing/changed files via mtime+size comparison and
//   request only what's needed.  No need to verify individual files
//   exist; that's Pipeline Sync's job.
// ---------------------------------------------------------------
static bool has_tree_cache(const std::string& root) {
    fs::path fastcp = fs::path(root) / ".fastcp";
    std::error_code ec;
    if (fs::exists(fastcp, ec)) {
        for (auto& de : fs::directory_iterator(fastcp, ec)) {
            std::string name = de.path().filename().string();
            if (name.rfind("treecache_", 0) == 0) return true;
        }
    }
    // Legacy fallback: old installs used dst/.fastcp_treecache
    return fs::exists(fs::path(root) / ".fastcp_treecache", ec);
}

// Check if there is any interrupted VA progress file under dst/.fastcp/
static bool has_any_va_progress(const std::string& root) {
    fs::path fastcp = fs::path(root) / ".fastcp";
    std::error_code ec;
    if (!fs::exists(fastcp, ec)) {
        // Legacy: old installs used dst/.fastcp_va_progress
        return fs::exists(fs::path(root) / ".fastcp_va_progress", ec);
    }
    for (auto& de : fs::directory_iterator(fastcp, ec)) {
        std::string name = de.path().filename().string();
        if (name.rfind("va_progress_", 0) == 0) return true;
    }
    return false;
}

ClientApp::ClientApp(const std::string& dst_dir,
                     const std::string& server_ip,
                     u16 server_port,
                     int retry_secs,
                     int num_conns)
    : dst_dir_(dst_dir)
    , server_ip_(server_ip)
    , server_port_(server_port)
    , retry_secs_(retry_secs)
    , num_conns_(num_conns > 0 ? num_conns : 4)
    , session_mgr_(dst_dir)
{
    fs::create_directories(dst_dir);
    LOG_INFO("ClientApp: dst_dir=" + dst_dir +
             " server=" + server_ip + ":" + std::to_string(server_port) +
             " conns=" + std::to_string(num_conns_));
}

ClientApp::~ClientApp() {
    stop();
}

void ClientApp::stop() {
    stop_.store(true);
}

// ---------------------------------------------------------------
// connect_with_retry
// ---------------------------------------------------------------

bool ClientApp::connect_with_retry(int num_conns,
                                   std::vector<TcpSocket>& sockets_out)
{
    using clock = std::chrono::steady_clock;
    auto deadline = clock::now() +
                    std::chrono::seconds(retry_secs_ > 0 ? retry_secs_ : 1);

    int delay_ms = 500;
    const int max_delay_ms = 8000;

    while (!stop_.load()) {
        std::vector<TcpSocket> tmp;
        tmp.reserve((size_t)num_conns);
        bool ok = true;

        try {
            for (int i = 0; i < num_conns; ++i) {
                TcpSocket s;
                s.connect(server_ip_, server_port_);
                s.tune();
                tmp.push_back(std::move(s));
            }
        } catch (const std::exception& e) {
            ok = false;
            if (clock::now() >= deadline) {
                LOG_ERROR("connect_with_retry: timed out (" +
                          std::string(e.what()) + ")");
                return false;
            }
            std::cerr << "[fastcp] server not ready, retry in "
                      << delay_ms / 1000.0 << "s\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
            delay_ms = std::min(delay_ms * 2, max_delay_ms);
        }

        if (ok) {
            sockets_out = std::move(tmp);
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------
// run
// ---------------------------------------------------------------

int ClientApp::run() {
    LOG_INFO("Connecting to server " + server_ip_ + ":" +
             std::to_string(server_port_) + " ...");

    // --- Step 1: connect first socket with retry ---
    std::vector<TcpSocket> first_vec;
    if (!connect_with_retry(1, first_vec)) {
        LOG_ERROR("Failed to connect to server");
        return 1;
    }
    TcpSocket& conn0 = first_vec[0];

    // --- Step 2: send HANDSHAKE_REQ on conn[0] ---
    // Advertise the actual number of connections we want.
    // Server groups connections by session_id; first connection uses
    // session_id=0 (new session) and tells server our desired num_connections.
    // Server responds with HANDSHAKE_ACK containing the session_id and
    // accepted_conns. Subsequent connections carry that session_id.

    // ---- Determine sync mode ----
    // Check for interrupted VA transfer first (highest priority).
    bool has_va_progress = has_any_va_progress(dst_dir_);

    // Check if the first file from the previous sync exists in dst.
    // This is more robust than "is dst empty?" — the destination might
    // contain unrelated files, but only a previously-synced destination
    // will have the FIRST file (files are scanned in a fixed order).
    //
    //   has_va_progress          → VA resume  (interrupted VA transfer)
    //   first file exists        → Pipeline Sync  (incremental update)
    //   otherwise (no cache /
    //     first file missing)    → VA full  (first sync or wrong dst)
    bool use_pipeline_sync = !has_va_progress && has_tree_cache(dst_dir_);

    u16 caps = CAP_RESUME | CAP_BUNDLE | CAP_COMPRESS | CAP_DELTA |
               CAP_VIRTUAL_ARCHIVE | CAP_CHUNK_RESUME | CAP_TREE_CACHE | CAP_COMPRESSED_TREE;
    if (use_pipeline_sync) caps |= CAP_PIPELINE_SYNC;
    LOG_INFO("Sync mode: " + std::string(
        has_va_progress   ? "Virtual Archive (resume)" :
        use_pipeline_sync ? "Pipeline Sync (incremental)" :
                            "Virtual Archive (full)"));

    HandshakeReq req0{};
    handshake_req_init(req0,
        /*session_id=*/0,           // 0 = new session
        /*num_conns=*/(u8)num_conns_,  // tell server how many connections to expect
        /*conn_idx=*/0,
        caps);
    proto::encode_handshake_req(req0);
    conn0.write_frame(MsgType::MT_HANDSHAKE_REQ, 0, &req0, sizeof(req0));

    // --- Step 3: receive HANDSHAKE_ACK ---
    FrameHeader hdr{};
    std::vector<u8> payload;
    if (!conn0.read_frame(hdr, payload)) {
        LOG_ERROR("No handshake response from server");
        return 1;
    }
    if ((MsgType)hdr.msg_type != MsgType::MT_HANDSHAKE_ACK ||
        payload.size() < sizeof(HandshakeAck))
    {
        std::string err(payload.begin(), payload.end());
        LOG_ERROR("Server rejected handshake: " + err);
        return 1;
    }

    HandshakeAck ack0{};
    std::memcpy(&ack0, payload.data(), sizeof(HandshakeAck));
    proto::decode_handshake_ack(ack0);

    u64 session_id   = ack0.session_id;
    int total_conns  = (int)ack0.accepted_conns;
    if (total_conns < 1) total_conns = 1;
    u32 chunk_size   = ack0.chunk_size_kb * 1024;
    if (chunk_size == 0) chunk_size = DEFAULT_CHUNK_SIZE;

    LOG_INFO("Handshake OK: session=" + std::to_string(session_id) +
             " conns=" + std::to_string(total_conns) +
             " chunk=" + utils::format_bytes(chunk_size));

    // --- Step 4: create session ---
    auto session = session_mgr_.create_session(
        total_conns, ack0.capabilities, chunk_size);
    // Override the auto-generated session_id with the server's
    session->session_id = session_id;

    // --- Step 5: connect remaining (total_conns-1) sockets ---
    std::vector<TcpSocket> extra;
    if (total_conns > 1) {
        if (!connect_with_retry(total_conns - 1, extra)) {
            LOG_ERROR("Failed to connect extra connections");
            return 1;
        }
        for (int i = 1; i < total_conns; ++i) {
            TcpSocket& si = extra[(size_t)(i - 1)];
            HandshakeReq req_i{};
            handshake_req_init(req_i, session_id,
                               (u8)total_conns, (u16)i, caps);
            proto::encode_handshake_req(req_i);
            si.write_frame(MsgType::MT_HANDSHAKE_REQ, 0, &req_i, sizeof(req_i));

            FrameHeader hdr_i{};
            std::vector<u8> pl_i;
            if (si.read_frame(hdr_i, pl_i)) {
                if ((MsgType)hdr_i.msg_type != MsgType::MT_HANDSHAKE_ACK) {
                    LOG_WARN("conn[" + std::to_string(i) + "] handshake NACK");
                }
            }
            LOG_INFO("conn[" + std::to_string(i) + "] connected");
        }
    }

    // Update session's total_conns so SESSION_DONE tracking is correct
    session->total_conns = total_conns;
    // conn_count starts at 1 (from create_session); increment for each extra conn
    for (int i = 1; i < total_conns; ++i) {
        session->conn_count.fetch_add(1);
    }

    // --- Step 6: create shared FileReceiver ---
    {
        std::lock_guard<std::mutex> lk(session->receiver_mutex);
        auto r = std::make_shared<FileReceiver>(session);
        session->shared_receiver = r;
    }

    // --- Step 7: run ConnectionHandlers ---
    // Use skip_handshake=true since we already did the handshake above.

    // Setup client-side TUI progress bar
    TuiState tui_state;
    tui_state.transfer_label = "Recv";
    auto tui = std::make_unique<Tui>(tui_state);

    // Bridge thread: periodically copies session stats -> TUI state.
    std::atomic<bool> bridge_stop{false};
    std::thread bridge([&]() {
        while (!bridge_stop.load()) {
            tui_state.bytes_sent.store(session->bytes_received.load());
            tui_state.bytes_total.store(session->bytes_total.load());
            tui_state.files_done.store(session->files_done.load());
            tui_state.files_total.store(session->files_total.load());
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        // Final update
        tui_state.bytes_sent.store(session->bytes_received.load());
        tui_state.bytes_total.store(session->bytes_total.load());
        tui_state.files_done.store(session->files_done.load());
        tui_state.files_total.store(session->files_total.load());
    });

    tui->start();

    std::vector<std::thread> threads;

    // conn[0]: file_list + transfer
    threads.emplace_back([&]() {
        try {
            ConnectionHandler h(std::move(conn0), session, 0, true);
            h.run();
        } catch (const std::exception& e) {
            LOG_ERROR("conn[0]: " + std::string(e.what()));
        }
    });

    // conn[1..N-1]: transfer only
    for (int i = 1; i < total_conns; ++i) {
        threads.emplace_back([&, i]() {
            try {
                ConnectionHandler h(std::move(extra[(size_t)(i - 1)]),
                                    session, i, true);
                h.run();
            } catch (const std::exception& e) {
                LOG_ERROR("conn[" + std::to_string(i) + "]: " + e.what());
            }
        });
    }

    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    bridge_stop.store(true);
    if (bridge.joinable()) bridge.join();
    tui->stop();

    u64 total_received = session->bytes_received.load();
    u32 files_done     = session->files_done.load();
    u32 files_total    = session->files_total.load();

    if (total_received == 0 && files_total > 0) {
        std::cout << "All " << files_total
                  << " files already up to date, nothing to transfer.\n";
    } else {
        std::cout << "Transfer complete: "
                  << utils::format_bytes(total_received)
                  << " in " << files_done << " files\n";
    }

    return session->done.load() ? 0 : 1;
}
