// ============================================================
// server_app.cpp -- FastCP server daemon implementation
// ============================================================

#include "server_app.hpp"
#include "archive_builder.hpp"
#include "../common/protocol_io.hpp"
#include "../common/file_io.hpp"
#include "../common/hash.hpp"
#include "../common/compress.hpp"
#include "../common/logger.hpp"
#include "../common/utils.hpp"
#include "../common/write_buffer.hpp"
#include <thread>
#include <algorithm>
#include <cstring>
#include <vector>
#include <deque>
#include <chrono>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <random>

namespace fs = std::filesystem;

// Load or create a 16-byte UUID for the source directory.
// Stored in src_dir/.fastcp/dir_id (created on first run).
// This UUID lets the client distinguish different source directories
// even when served from the same IP:port.
static void load_or_create_dir_id(const std::string& src_dir, u8 out[16]) {
    fs::path fastcp_dir = fs::path(src_dir) / ".fastcp";
    fs::path id_file    = fastcp_dir / "dir_id";

    // Try to read existing id
    {
        std::ifstream f(id_file.string(), std::ios::binary);
        if (f) {
            f.read((char*)out, 16);
            if (f.gcount() == 16) return;
        }
    }

    // Generate new random UUID
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<u64> dist;
    u64 a = dist(gen), b = dist(gen);
    std::memcpy(out,     &a, 8);
    std::memcpy(out + 8, &b, 8);

    // Persist it
    std::error_code ec;
    fs::create_directories(fastcp_dir, ec);
    std::ofstream f(id_file.string(), std::ios::binary | std::ios::trunc);
    if (f) f.write((char*)out, 16);
}

// ============================================================
// ServerApp
// ============================================================

ServerApp::ServerApp(ServerConfig config)
    : config_(std::move(config))
{}

ServerApp::~ServerApp() {
    stop();
}

int ServerApp::run() {
    // Load (or create) the source-directory UUID so every session can send it
    // to the client for per-directory temp-file naming.
    load_or_create_dir_id(config_.src_dir, config_.dir_id);

    // Scan source directory: stat-only (no hash computation) so the server
    // starts listening almost immediately regardless of directory size.
    LOG_INFO("Scanning source directory: " + config_.src_dir);
    DirScanner scanner(config_.src_dir);
    scanner.start_async();
    {
        std::lock_guard<std::mutex> lk(entries_mutex_);
        entries_     = scanner.get_all();   // fast: only stat() calls now
        total_bytes_ = scanner.total_bytes();
    }

    if (entries_.empty()) {
        LOG_WARN("Source directory is empty.");
    } else {
        LOG_INFO("Ready: " + std::to_string(entries_.size()) +
                 " files (" + utils::format_bytes(total_bytes_) + ")" +
                 " – computing hashes in background");
    }

    // Background thread: compute file hashes so subsequent sessions benefit
    // from cached values.  Uses a local copy to avoid holding entries_mutex_
    // while doing I/O, then flushes computed hashes back under lock.
    std::thread bg_hasher([this]() {
        std::vector<FileEntry> local;
        {
            std::lock_guard<std::mutex> lk(entries_mutex_);
            local = entries_;
        }
        for (auto& fe : local) {
            // Stop early if a client session has started: phase_transfer_archive
            // will call pre_read_all() which computes hashes in the same pass as
            // the file read, making the background hasher redundant and harmful
            // (it competes for disk I/O with the transfer).
            if (bg_hasher_stop_.load(std::memory_order_relaxed)) {
                LOG_INFO("Background hash computation cancelled (transfer started)");
                return;
            }
            if (fe.hash_computed) continue;
            if (fe.file_size == 0) {
                fe.xxh3_128     = hash::xxh3_128(nullptr, 0);
                fe.hash_computed = true;
            } else {
                try {
                    file_io::MmapReader reader(fe.abs_path);
                    fe.xxh3_128     = hash::xxh3_128(reader.data(), (size_t)reader.size());
                    fe.hash_computed = true;
                } catch (...) {}
            }
        }
        // Flush computed hashes back to entries_
        std::lock_guard<std::mutex> lk(entries_mutex_);
        for (size_t i = 0; i < entries_.size() && i < local.size(); ++i) {
            if (local[i].hash_computed && !entries_[i].hash_computed) {
                entries_[i].xxh3_128     = local[i].xxh3_128;
                entries_[i].hash_computed = true;
            }
        }
        LOG_INFO("Background hash computation complete");
    });
    bg_hasher.detach();

    // Bind and listen
    listen_sock_.bind_and_listen(config_.listen_ip, config_.listen_port);
    running_.store(true);

    LOG_INFO("FastCP server listening on " +
             config_.listen_ip + ":" + std::to_string(config_.listen_port) +
             "  (concurrent multi-client mode)");

    accept_loop();

    // Wait for all connector threads to finish
    {
        std::lock_guard<std::mutex> lk(connector_mutex_);
        for (auto& t : connector_threads_) {
            if (t.joinable()) t.join();
        }
    }
    return 0;
}

void ServerApp::stop() {
    running_.store(false);
    listen_sock_.close();
    cleanup_threads();
}

// ---------------------------------------------------------------
// accept_loop
//   Pure accept() loop. For every accepted socket we spawn a
//   short-lived connector_thread to do the handshake and grouping.
//   This means accept_loop NEVER blocks on a single client's extra
//   connections -- multiple clients can connect concurrently.
// ---------------------------------------------------------------
void ServerApp::accept_loop() {
    while (running_.load()) {
        try {
            TcpSocket sock = listen_sock_.accept();
            sock.tune();

            std::string peer = sock.peer_addr();
            LOG_DEBUG("Accepted socket from " + peer);

            // Spawn a connector thread for this socket; it will:
            // 1. read the HANDSHAKE_REQ
            // 2. group it with other sockets from the same session
            // 3. when all N connections are in, launch a ClientSession
            {
                std::lock_guard<std::mutex> lk(connector_mutex_);
                connector_threads_.emplace_back(
                    [this, s = std::move(sock)]() mutable {
                        connector_thread(std::move(s));
                    }
                );
            }

            // Periodically reap finished connector/session threads
            cleanup_threads();

        } catch (const std::exception& e) {
            if (!running_.load()) break;
            LOG_ERROR("accept_loop: " + std::string(e.what()));
        }
    }
}

// ---------------------------------------------------------------
// connector_thread
//   Short-lived thread: reads HANDSHAKE_REQ from one socket and
//   places it into the correct PendingSession bucket.
//
//   Protocol for first connection of a session:
//     session_id == 0  → new session; server generates session_id,
//                        sends HANDSHAKE_ACK immediately.
//   Protocol for subsequent connections:
//     session_id != 0  → looks up existing PendingSession,
//                        appends socket, sends HANDSHAKE_ACK.
//   When all N connections for a session have arrived, launches
//   a full ClientSession thread.
// ---------------------------------------------------------------
void ServerApp::connector_thread(TcpSocket sock) {
    std::string peer = sock.peer_addr();
    try {
        sock.set_recv_timeout_ms(8000); // 8 s to receive handshake

        // Read HANDSHAKE_REQ
        PeekedFrame pf;
        if (!sock.read_frame(pf.hdr, pf.payload)) {
            LOG_WARN("connector_thread: no frame from " + peer);
            return;
        }

        if ((MsgType)pf.hdr.msg_type != MsgType::MT_HANDSHAKE_REQ ||
            pf.payload.size() < sizeof(HandshakeReq))
        {
            LOG_WARN("connector_thread: bad frame from " + peer);
            return;
        }

        HandshakeReq req{};
        std::memcpy(&req, pf.payload.data(), sizeof(HandshakeReq));
        proto::decode_handshake_req(req);

        if (!handshake_req_valid_magic(req)) {
            LOG_WARN("connector_thread: bad FCP magic from " + peer);
            return;
        }

        sock.set_recv_timeout_ms(0); // remove timeout for data phase

        // ---- Group this socket by session_id ----
        // IMPORTANT: We must send HANDSHAKE_ACK BEFORE releasing the lock
        // so the client can proceed to connect the next sockets.
        // Capability negotiation is done on conn[0] and stored in PendingSession.
        std::unique_lock<std::mutex> lk(pending_mutex_);

        u64 sid = req.session_id;
        int num_conns = std::max(1, (int)req.num_connections);
        u16 agreed_caps = 0;
        u32 agreed_chunk_size = DEFAULT_CHUNK_SIZE;

        if (sid == 0) {
            // First connection of a new session: generate session_id,
            // negotiate capabilities, send ACK immediately.
            sid = generate_session_id();

        u16 server_caps = CAP_COMPRESS | CAP_RESUME | CAP_BUNDLE | CAP_DELTA | CAP_VIRTUAL_ARCHIVE
                        | CAP_CHUNK_RESUME | CAP_PIPELINE_SYNC | CAP_TREE_CACHE | CAP_COMPRESSED_TREE;
            if (!config_.use_compress) server_caps &= ~CAP_COMPRESS;
            agreed_caps       = req.capabilities & server_caps;
            agreed_chunk_size = config_.chunk_size;

            LOG_INFO("New session " + std::to_string(sid) +
                     " from " + peer +
                     " (expecting " + std::to_string(num_conns) + " conn)");

            PendingSession ps;
            ps.session_id         = sid;
            ps.expected_conns     = num_conns;
            ps.agreed_caps        = agreed_caps;
            ps.agreed_chunk_size  = agreed_chunk_size;
            ps.created_at         = std::chrono::steady_clock::now();
            ps.sockets.push_back(std::move(sock));
            ps.peeked.push_back(std::move(pf));
            pending_[sid] = std::move(ps);
        } else {
            // Additional connection for an existing session
            auto it = pending_.find(sid);
            if (it == pending_.end()) {
                LOG_WARN("connector_thread: unknown session_id " +
                         std::to_string(sid) + " from " + peer);
                return;
            }
            agreed_caps       = it->second.agreed_caps;
            agreed_chunk_size = it->second.agreed_chunk_size;

            LOG_DEBUG("Session " + std::to_string(sid) +
                      " +conn[" + std::to_string(it->second.sockets.size()) +
                      "] from " + peer);
            it->second.sockets.push_back(std::move(sock));
            it->second.peeked.push_back(std::move(pf));
        }

        // Send HANDSHAKE_ACK immediately on this socket so the client
        // can proceed without waiting for all N connections to arrive.
        // We already moved sock into pending_, so get it back via the map.
        {
            auto it = pending_.find(sid);
            if (it != pending_.end() && !it->second.sockets.empty()) {
                TcpSocket& s = it->second.sockets.back();
                HandshakeAck ack{};
                ack.session_id     = sid;
                ack.accepted_conns = (u16)it->second.expected_conns;
                ack.capabilities   = agreed_caps;
                ack.chunk_size_kb  = agreed_chunk_size / 1024;
                proto::encode_handshake_ack(ack);
                try {
                    s.write_frame(MsgType::MT_HANDSHAKE_ACK, 0, &ack, sizeof(ack));
                } catch (const std::exception& e) {
                    LOG_WARN("connector_thread: ACK send failed for " + peer +
                             ": " + e.what());
                }
            }
        }

        // Check if session is complete (all N connections arrived)
        auto it2 = pending_.find(sid);
        if (it2 != pending_.end() &&
            (int)it2->second.sockets.size() >= it2->second.expected_conns)
        {
            PendingSession ps = std::move(it2->second);
            pending_.erase(it2);
            lk.unlock();

            LOG_INFO("Session " + std::to_string(ps.session_id) +
                     " complete (" + std::to_string(ps.sockets.size()) +
                     " conn) -- launching ClientSession");
            launch_session(std::move(ps));
        }

    } catch (const std::exception& e) {
        LOG_ERROR("connector_thread (" + peer + "): " + e.what());
    }
}

// ---------------------------------------------------------------
// launch_session
//   Fires a new ClientSession thread for a fully-assembled session.
// ---------------------------------------------------------------
void ServerApp::launch_session(PendingSession ps) {
    // Stop the background hasher: pre_read_all() in phase_transfer_archive will
    // compute any missing hashes in the same pass as the file read, making the
    // background hasher redundant and a source of disk-I/O competition.
    bg_hasher_stop_.store(true, std::memory_order_relaxed);

    // Snapshot current file entries
    std::vector<FileEntry> entries_snap;
    u64 total_bytes_snap;
    {
        std::lock_guard<std::mutex> lk(entries_mutex_);
        entries_snap     = entries_;
        total_bytes_snap = total_bytes_;
    }

    u64 sid   = ps.session_id;
    u16 caps  = ps.agreed_caps;
    u32 chunk = ps.agreed_chunk_size;
    {
        std::lock_guard<std::mutex> lk(session_threads_mutex_);
        session_threads_.emplace_back(
            [this,
             es    = std::move(entries_snap),
             tb    = total_bytes_snap,
             so    = std::move(ps.sockets),
             pf    = std::move(ps.peeked),
             sid, caps, chunk]() mutable {
                handle_client(std::move(es), tb, std::move(so), std::move(pf),
                               sid, caps, chunk);
            }
        );
    }
}

void ServerApp::handle_client(
    std::vector<FileEntry> entries,
    u64 total_bytes,
    std::vector<TcpSocket> sockets,
    std::vector<PeekedFrame> peeked,
    u64 session_id,
    u16 agreed_caps,
    u32 agreed_chunk_size)
{
    try {
        ClientSession sess(config_,
                           std::move(entries),
                           total_bytes,
                           std::move(sockets),
                           std::move(peeked),
                           session_id,
                           agreed_caps,
                           agreed_chunk_size);
        sess.run();
    } catch (const std::exception& e) {
        LOG_ERROR("Client session error: " + std::string(e.what()));
    }
}

void ServerApp::cleanup_threads() {
    // Reap connector threads: detach each, then clear the vector.
    // Using an explicit loop instead of remove_if with a side-effecting
    // predicate avoids undefined behavior when the compiler reorders or
    // re-evaluates predicate calls during element moves.
    {
        std::lock_guard<std::mutex> lk(connector_mutex_);
        for (auto& t : connector_threads_) {
            if (t.joinable()) t.detach();
        }
        connector_threads_.clear();
    }
    // Reap session threads
    {
        std::lock_guard<std::mutex> lk(session_threads_mutex_);
        for (auto& t : session_threads_) {
            if (t.joinable()) t.detach();
        }
        session_threads_.clear();
    }
    // Expire stale pending sessions (> 30 s without all connections)
    {
        std::lock_guard<std::mutex> lk(pending_mutex_);
        auto now = std::chrono::steady_clock::now();
        for (auto it = pending_.begin(); it != pending_.end(); ) {
            auto age = std::chrono::duration_cast<std::chrono::seconds>(
                now - it->second.created_at).count();
            if (age > 30) {
                LOG_WARN("Expiring stale pending session " +
                         std::to_string(it->first) +
                         " (only " + std::to_string(it->second.sockets.size()) +
                         "/" + std::to_string(it->second.expected_conns) +
                         " conn arrived)");
                it = pending_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

u64 ServerApp::generate_session_id() {
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    u64 sid = (u64)now ^ ((u64)(uintptr_t)this ^ (session_id_counter_.fetch_add(1) << 20));
    return sid == 0 ? 1 : sid;
}

// ============================================================
// ClientSession
// ============================================================

ClientSession::ClientSession(
    const ServerConfig& cfg,
    std::vector<FileEntry> entries,
    u64 total_bytes,
    std::vector<TcpSocket> sockets,
    std::vector<PeekedFrame> peeked,
    u64 session_id,
    u16 agreed_caps,
    u32 agreed_chunk_size)
    : cfg_(cfg)
    , entries_(std::move(entries))
    , total_bytes_(total_bytes)
    , peeked_frames_(std::move(peeked))
    , session_id_(session_id)
    , agreed_caps_(agreed_caps)
    , agreed_chunk_size_(agreed_chunk_size ? agreed_chunk_size : cfg.chunk_size)
{
    tui_ = std::make_unique<Tui>(tui_state_);
    pool_.init_from_accepted(std::move(sockets));
}

int ClientSession::run() {
    tui_state_.files_total.store((u32)entries_.size());
    tui_state_.bytes_total.store(total_bytes_);
    tui_state_.active.store(true);
    tui_->start();

    if (!phase_handshake()) {
        LOG_ERROR("Handshake failed");
        tui_->stop();
        return 1;
    }

    if (agreed_caps_ & CAP_PIPELINE_SYNC) {
        // Pipeline sync: server streams file tree while client sends WANT_FILE,
        // decoupling local file checking from network transfer.
        if (!phase_pipeline_sync()) {
            LOG_WARN("Pipeline sync completed with some errors");
        }
    } else if (agreed_caps_ & CAP_VIRTUAL_ARCHIVE) {
        // VA direct mode: skip file list exchange entirely.
        // Build an all-FULL plan (client dst is empty or a fresh transfer)
        // and go straight to the archive manifest + chunk stream.
        SyncPlanMap plan;
        for (auto& fe : entries_)
            plan.plan[fe.file_id] = {SyncAction::FULL, 0};
        if (!phase_transfer_archive(plan)) {
            LOG_WARN("Archive transfer completed with some errors");
        }
    } else {
        SyncPlanMap plan;
        if (!phase_file_list(plan)) {
            LOG_ERROR("File list exchange failed");
            tui_->stop();
            return 1;
        }
        if (!phase_transfer(plan)) {
            LOG_WARN("Transfer completed with some errors");
        }
    }

    phase_done();
    tui_->stop();

    u64 bytes = tui_state_.bytes_sent.load();
    std::cout << "\nTransfer complete: "
              << utils::format_bytes(bytes)
              << " in " << tui_state_.files_done.load() << " files\n";
    return 0;
}

// ---- Phase 1: Handshake ----

bool ClientSession::phase_handshake() {
    int num_conns = pool_.size();
    for (int i = 0; i < num_conns; ++i) {
        if (!do_handshake_peeked(i)) return false;
    }
    return true;
}

bool ClientSession::do_handshake_peeked(int conn_idx) {
    // Note: HANDSHAKE_ACK was already sent by connector_thread when each
    // socket arrived. Here we just validate the peeked frame for logging.
    if (conn_idx >= (int)peeked_frames_.size()) {
        LOG_ERROR("No peeked frame for conn " + std::to_string(conn_idx));
        return false;
    }

    auto& pf = peeked_frames_[(size_t)conn_idx];

    if ((MsgType)pf.hdr.msg_type != MsgType::MT_HANDSHAKE_REQ ||
        pf.payload.size() < sizeof(HandshakeReq))
    {
        LOG_ERROR("conn[" + std::to_string(conn_idx) + "]: bad peeked frame");
        return false;
    }

    HandshakeReq req{};
    std::memcpy(&req, pf.payload.data(), sizeof(HandshakeReq));
    proto::decode_handshake_req(req);

    if (!handshake_req_valid_magic(req)) {
        LOG_ERROR("conn[" + std::to_string(conn_idx) + "]: bad magic");
        return false;
    }
    if (req.version != FASTCP_VERSION) {
        LOG_ERROR("conn[" + std::to_string(conn_idx) + "]: version mismatch " +
                  std::to_string(req.version));
        return false;
    }

    LOG_INFO("Handshake validated: conn=" + std::to_string(conn_idx) +
             " session=" + std::to_string(session_id_) +
             " caps=0x" + [](u16 v){ char buf[8]; snprintf(buf,sizeof(buf),"%04x",v); return std::string(buf); }(agreed_caps_));
    return true;
}

// ---- Phase 2: File list + sync plan ----

bool ClientSession::phase_file_list(SyncPlanMap& plan_out) {
    TcpSocket& sock = pool_.get(0);

    // Send file list; compute hash on-demand for entries not yet hashed.
    // Each ClientSession owns its own copy of entries_, so no cross-session locking.
    sock.write_frame(MsgType::MT_FILE_LIST_BEGIN, 0, nullptr, 0);

    for (auto& fe : entries_) {
        // Lazy hash computation: only happens for files the background hasher
        // hasn't reached yet.  Result is cached in this session's local copy.
        if (!fe.hash_computed) {
            if (fe.file_size == 0) {
                fe.xxh3_128     = hash::xxh3_128(nullptr, 0);
                fe.hash_computed = true;
            } else {
                try {
                    file_io::MmapReader reader(fe.abs_path);
                    fe.xxh3_128     = hash::xxh3_128(reader.data(), (size_t)reader.size());
                    fe.hash_computed = true;
                } catch (const std::exception& e) {
                    LOG_WARN("Cannot hash " + fe.abs_path + ": " + e.what());
                }
            }
        }

        FileListEntry fle{};
        fle.file_id   = fe.file_id;
        fle.file_size = fe.file_size;
        fle.mtime_ns  = fe.mtime_ns;
        fle.path_len  = (u16)fe.rel_path.size();
        fle.flags     = 0;
        hash::to_bytes(fe.xxh3_128, fle.xxh3_128);

        std::vector<u8> payload(sizeof(FileListEntry) + fe.rel_path.size());
        FileListEntry encoded = fle;
        proto::encode_file_list_entry(encoded);
        std::memcpy(payload.data(), &encoded, sizeof(FileListEntry));
        std::memcpy(payload.data() + sizeof(FileListEntry),
                    fe.rel_path.data(), fe.rel_path.size());

        sock.write_frame(MsgType::MT_FILE_LIST_ENTRY, 0,
                         payload.data(), (u32)payload.size());
    }
    sock.write_frame(MsgType::MT_FILE_LIST_END, 0, nullptr, 0);

    // Receive sync plan
    FrameHeader hdr{};
    std::vector<u8> payload;
    if (!sock.read_frame(hdr, payload)) return false;
    if ((MsgType)hdr.msg_type != MsgType::MT_SYNC_PLAN_BEGIN) return false;

    for (;;) {
        if (!sock.read_frame(hdr, payload)) return false;
        if ((MsgType)hdr.msg_type == MsgType::MT_SYNC_PLAN_END) break;
        if ((MsgType)hdr.msg_type != MsgType::MT_SYNC_PLAN_ENTRY) continue;

        if (payload.size() < sizeof(SyncPlanEntry)) continue;
        SyncPlanEntry spe{};
        std::memcpy(&spe, payload.data(), sizeof(SyncPlanEntry));
        proto::decode_sync_plan_entry(spe);
        plan_out.plan[spe.file_id] = {(SyncAction)spe.action, spe.resume_offset};
    }

    int skip = 0, full = 0, partial = 0;
    for (auto& [fid, p] : plan_out.plan) {
        switch (p.first) {
            case SyncAction::SKIP:    ++skip;    break;
            case SyncAction::FULL:    ++full;    break;
            case SyncAction::PARTIAL: ++partial; break;
            default: break;
        }
    }
    LOG_INFO("Sync plan: skip=" + std::to_string(skip) +
             " full=" + std::to_string(full) +
             " partial=" + std::to_string(partial));

    // ---- Read optional block checksums for delta sync ----
    // The client sends zero or more MT_BLOCK_CHECKSUMS followed by MT_BLOCK_CHECKSUMS_END.
    // No timeout needed - we just read until we get the END marker.
    if (agreed_caps_ & CAP_DELTA) {
        for (;;) {
            FrameHeader bhdr{};
            std::vector<u8> bpl;
            if (!sock.read_frame(bhdr, bpl)) {
                LOG_WARN("Connection lost during checksum phase");
                return false;
            }

            MsgType mt = (MsgType)bhdr.msg_type;

            if (mt == MsgType::MT_BLOCK_CHECKSUMS_END) {
                // Client signaled end of checksum stream
                break;
            }

            if (mt != MsgType::MT_BLOCK_CHECKSUMS) {
                LOG_WARN("Unexpected message during checksum phase: " + std::to_string(bhdr.msg_type));
                // Not a checksum - protocol error, but try to continue
                break;
            }

            if (bpl.size() < sizeof(BlockChecksumMsg)) continue;

            BlockChecksumMsg bc_hdr{};
            std::memcpy(&bc_hdr, bpl.data(), sizeof(BlockChecksumMsg));
            proto::decode_block_checksum_msg(bc_hdr);

            u32 fid   = bc_hdr.file_id;
            u32 bcnt  = bc_hdr.block_count;
            size_t expected_sz = sizeof(BlockChecksumMsg) + (size_t)bcnt * sizeof(BlockChecksumEntry);
            if (bpl.size() < expected_sz) continue;

            auto& ev = delta_checksums_[fid];
            ev.resize(bcnt);
            for (u32 i = 0; i < bcnt; ++i) {
                std::memcpy(&ev[i],
                    bpl.data() + sizeof(BlockChecksumMsg) + i * sizeof(BlockChecksumEntry),
                    sizeof(BlockChecksumEntry));
                proto::decode_block_checksum_entry(ev[i]);
            }
            delta_block_sizes_[fid] = bc_hdr.block_size;
            LOG_DEBUG("Delta: " + std::to_string(bcnt) + " checksums for file_id=" + std::to_string(fid));
        }
        LOG_INFO("Delta checksums for " + std::to_string(delta_checksums_.size()) + " files");
    }

    u64 skip_bytes = 0;
    for (auto& fe : entries_) {
        auto it = plan_out.plan.find(fe.file_id);
        if (it != plan_out.plan.end() && it->second.first == SyncAction::SKIP) {
            skip_bytes += fe.file_size;
        }
    }
    tui_state_.bytes_total.fetch_sub(skip_bytes);
    tui_state_.files_done.fetch_add((u32)skip);
    return true;
}

// ---- Phase 3: Transfer ----

bool ClientSession::phase_transfer(const SyncPlanMap& plan) {
    FileSender sender(pool_, tui_state_, cfg_.use_compress, agreed_chunk_size_,
                      delta_checksums_, delta_block_sizes_);
    if (agreed_caps_ & CAP_CHUNK_RESUME) {
        sender.set_chunk_resume(true);
    }
    if (cfg_.max_chunks > 0) {
        sender.set_max_chunks(cfg_.max_chunks);
    }

    // Separate small files (bundles) and large files (parallel)
    std::vector<const FileEntry*> small_files;
    std::vector<std::pair<const FileEntry*, u64>> large_files; // (entry, resume_offset)

    for (auto& fe : entries_) {
        auto it = plan.plan.find(fe.file_id);
        if (it != plan.plan.end() && it->second.first == SyncAction::SKIP) {
            continue;
        }
        u64 resume_offset = 0;
        if (it != plan.plan.end() && it->second.first == SyncAction::PARTIAL) {
            resume_offset = it->second.second;
        }

        if (fe.is_small) {
            small_files.push_back(&fe);
        } else {
            large_files.emplace_back(&fe, resume_offset);
        }
    }

    int num_conns = pool_.size();
    bool all_ok = true;

    // ---- Send small files as bundles (parallel: each connection handles its own queue) ----
    // Assign files to connections round-robin, then launch one thread per connection
    // so all N connections send their bundle batches concurrently.
    if (!small_files.empty()) {
        // Partition files across connections
        std::vector<std::vector<const FileEntry*>> conn_files(num_conns);
        for (size_t i = 0; i < small_files.size(); ++i) {
            conn_files[i % (size_t)num_conns].push_back(small_files[i]);
        }

        std::vector<std::thread> bundle_threads;
        std::atomic<bool> any_failed{false};

        for (int ci = 0; ci < num_conns; ++ci) {
            if (conn_files[ci].empty()) continue;
            bundle_threads.emplace_back([&, ci]() {
                auto& files = conn_files[ci];
                std::vector<const FileEntry*> batch;
                u64 batch_size = 0;

                auto flush = [&]() {
                    if (batch.empty()) return;
                    if (!sender.send_bundle(batch, ci)) {
                        any_failed.store(true);
                    }
                    tui_state_.files_done.fetch_add((u32)batch.size());
                    batch.clear();
                    batch_size = 0;
                };

                for (auto* fe : files) {
                    batch.push_back(fe);
                    batch_size += fe->file_size;
                    if (batch.size() >= MAX_BUNDLE_FILES || batch_size >= MAX_BUNDLE_SIZE) {
                        flush();
                    }
                }
                flush();
            });
        }

        for (auto& t : bundle_threads) {
            if (t.joinable()) t.join();
        }

        if (any_failed.load()) all_ok = false;
    }

    // ---- Send large files in parallel ----
    // Smart scheduling:
    //   - Each file can use multiple connections for intra-file parallelism
    //   - When there are more files than connections, interleave them

    if (!large_files.empty()) {
        int num_files = (int)large_files.size();

        // Decide: how many files to send in parallel?
        // If more conns than files, each file gets multiple conns (intra-file parallelism)
        // If more files than conns, send multiple files in parallel (inter-file parallelism)

        int conns_per_file = std::max(1, num_conns / std::min(num_files, num_conns));
        int files_in_parallel = std::min(num_files, num_conns);

        // Process files in batches
        int files_sent = 0;
        while (files_sent < num_files) {
            int current_batch = std::min(files_in_parallel, num_files - files_sent);

            // Assign connections to files in this batch
            std::vector<std::vector<int>> file_conns(current_batch);
            int conn_idx = 0;
            for (int b = 0; b < current_batch; ++b) {
                for (int c = 0; c < conns_per_file && conn_idx < num_conns; ++c) {
                    file_conns[b].push_back(conn_idx++);
                }
            }

            // Initialize per-file tracking BEFORE spawning threads to avoid race condition.
            // (Previously done inside thread 0 with a 1ms sleep -- unreliable.)
            for (int b = 0; b < current_batch; ++b) {
                int fi = files_sent + b;
                auto& [fe_ptr, ro] = large_files[fi];
                int num_threads = (int)file_conns[b].size();
                sender.init_file_tracking(fe_ptr->file_id, num_threads);
            }

            // Launch parallel transfers for this batch
            std::vector<std::thread> threads;
            std::atomic<int> batch_files_done{0};
            std::atomic<int> batch_files_ok{0};
            std::mutex result_mutex;
            bool batch_all_ok = true;

            for (int b = 0; b < current_batch; ++b) {
                int fi = files_sent + b;
                auto& [fe, resume_offset] = large_files[fi];
                auto& conns = file_conns[b];
                int num_threads = (int)conns.size();

                // Each file gets its own set of threads (intra-file parallelism)
                for (int ti = 0; ti < num_threads; ++ti) {
                    threads.emplace_back([&, fe, resume_offset, conns, ti, num_threads]() {
                        try {
                            bool ok = sender.send_large_file_parallel(
                                *fe,
                                resume_offset,
                                conns,
                                num_threads,
                                ti,
                                [&](u32 /*file_id*/) {
                                    // Called when file complete (last thread only)
                                    tui_state_.files_done.fetch_add(1);
                                    batch_files_ok.fetch_add(1);
                                }
                            );
                            if (!ok) {
                                std::lock_guard<std::mutex> lk(result_mutex);
                                batch_all_ok = false;
                            }
                        } catch (const std::exception& e) {
                            LOG_ERROR("Parallel send error: " + std::string(e.what()));
                            std::lock_guard<std::mutex> lk(result_mutex);
                            batch_all_ok = false;
                        }
                    });
                }
            }

            // Wait for this batch
            for (auto& t : threads) {
                if (t.joinable()) t.join();
            }

            if (!batch_all_ok) {
                all_ok = false;
            }

            files_sent += current_batch;
        }
    }

    return all_ok;
}

// ---- Phase 3b: Archive Transfer ----

bool ClientSession::phase_transfer_archive(const SyncPlanMap& plan) {
    // 1. Filter out SKIP files; build the file list to archive
    std::vector<FileEntry> to_send;
    for (auto& fe : entries_) {
        auto it = plan.plan.find(fe.file_id);
        if (it != plan.plan.end() && it->second.first == SyncAction::SKIP) {
            continue;
        }
        to_send.push_back(fe);
    }

    // 2. Build virtual archive layout
    ArchiveBuilder archive;
    archive.build(to_send, agreed_chunk_size_);

    LOG_INFO("Archive mode: " + std::to_string(archive.files().size()) +
             " files, " + std::to_string(archive.chunks().size()) + " chunks");

    // 2b. Pre-read all file content into memory (one ReadFile/read() per file).
    //     This also computes per-file xxh3_128 hashes in the same pass so the
    //     manifest has correct hashes, and makes subsequent read_chunk() calls
    //     pure in-memory memcpy operations (no further disk I/O).
    //     Falls back to the original MmapReader path for archives > 256 MiB.
    if (!archive.pre_read_all()) {
        LOG_INFO("Archive too large for in-memory mode — using MmapReader per chunk");
    }

    TcpSocket& sock0 = pool_.get(0);

    // 3. Send manifest on conn[0]
    {
        ArchiveManifestHdr hdr_msg{};
        hdr_msg.total_virtual_size = archive.total_size();
        hdr_msg.total_files        = (u32)archive.files().size();
        hdr_msg.chunk_size         = archive.chunk_size();
        hdr_msg.total_chunks       = (u32)archive.chunks().size();
        std::memcpy(hdr_msg.dir_id, cfg_.dir_id, 16);
        ArchiveManifestHdr encoded = hdr_msg;
        proto::encode_archive_manifest_hdr(encoded);
        sock0.write_frame(MsgType::MT_ARCHIVE_MANIFEST_HDR, 0,
                          &encoded, sizeof(encoded));
    }

    // Send all file entries coalesced into a few large TCP writes.
    // Without TcpWriteBuffer, 10 000 entries = 10 000 write() syscalls
    // (~300 ms on loopback with TCP_NODELAY); with it, ~5 writes.
    {
        TcpWriteBuffer wbuf(sock0);
        for (const auto& vf : archive.files()) {
            std::vector<u8> payload(sizeof(ArchiveFileEntry) + vf.rel_path.size());
            ArchiveFileEntry fe_msg{};
            fe_msg.file_id        = vf.file_id;
            fe_msg.virtual_offset = vf.virtual_offset;
            fe_msg.file_size      = vf.file_size;
            fe_msg.mtime_ns       = vf.mtime_ns;
            fe_msg.path_len       = (u16)vf.rel_path.size();
            fe_msg.flags          = 0;
            hash::to_bytes(vf.xxh3_128, fe_msg.xxh3_128);
            ArchiveFileEntry encoded = fe_msg;
            proto::encode_archive_file_entry(encoded);
            std::memcpy(payload.data(), &encoded, sizeof(ArchiveFileEntry));
            std::memcpy(payload.data() + sizeof(ArchiveFileEntry),
                        vf.rel_path.data(), vf.rel_path.size());
            wbuf.write_frame(MsgType::MT_ARCHIVE_FILE_ENTRY, 0,
                             payload.data(), (u32)payload.size());
        }
        wbuf.write_frame(MsgType::MT_ARCHIVE_MANIFEST_END, 0, nullptr, 0);
        // destructor flushes all buffered data in one/two send() calls
    }

    // 4. Receive CHUNK_REQUEST from client (conn[0])
    FrameHeader req_hdr{};
    std::vector<u8> req_payload;
    if (!sock0.read_frame(req_hdr, req_payload)) {
        LOG_ERROR("phase_transfer_archive: connection closed waiting for CHUNK_REQUEST");
        return false;
    }
    if ((MsgType)req_hdr.msg_type != MsgType::MT_CHUNK_REQUEST) {
        LOG_ERROR("phase_transfer_archive: expected MT_CHUNK_REQUEST, got " +
                  std::to_string(req_hdr.msg_type));
        return false;
    }

    std::vector<u32> needed_chunks;
    if (req_payload.size() >= sizeof(ChunkRequestHdr)) {
        ChunkRequestHdr creq{};
        std::memcpy(&creq, req_payload.data(), sizeof(ChunkRequestHdr));
        proto::decode_chunk_request_hdr(creq);
        u32 cnt = creq.needed_count;

        size_t expected_size = sizeof(ChunkRequestHdr) + (size_t)cnt * sizeof(u32);
        if (req_payload.size() >= expected_size) {
            needed_chunks.reserve(cnt);
            const u8* id_ptr = req_payload.data() + sizeof(ChunkRequestHdr);
            for (u32 i = 0; i < cnt; ++i) {
                u32 cid;
                std::memcpy(&cid, id_ptr + i * sizeof(u32), sizeof(u32));
                needed_chunks.push_back(proto::ntoh32(cid));
            }
        }
    }

    LOG_INFO("CHUNK_REQUEST: " + std::to_string(needed_chunks.size()) +
             " chunks requested");

    // If no chunks needed, send ARCHIVE_DONE immediately on all connections
    if (needed_chunks.empty()) {
        int num_conns = pool_.size();
        for (int ci = 0; ci < num_conns; ++ci) {
            pool_.get(ci).write_frame(MsgType::MT_ARCHIVE_DONE, 0, nullptr, 0);
        }
        return true;
    }

    // 5. Distribute chunks round-robin across connections
    int num_conns = pool_.size();
    std::vector<std::vector<u32>> conn_chunks((size_t)num_conns);
    for (size_t i = 0; i < needed_chunks.size(); ++i) {
        conn_chunks[i % (size_t)num_conns].push_back(needed_chunks[i]);
    }

    // 6. Parallel send (one thread per connection)
    FileSender sender(pool_, tui_state_, cfg_.use_compress, agreed_chunk_size_,
                      delta_checksums_, delta_block_sizes_);
    std::vector<std::thread> threads;
    std::atomic<bool> any_failed{false};

    for (int ci = 0; ci < num_conns; ++ci) {
        threads.emplace_back([&, ci]() {
            if (!sender.send_archive_range(archive, conn_chunks[(size_t)ci], ci)) {
                any_failed.store(true);
            }
        });
    }
    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    return !any_failed.load();
}



// Compute a stable 128-bit token over the file tree: xxh3-128 of sorted
// (rel_path + mtime_ns + file_size) for every entry. Used by CAP_TREE_CACHE.
static hash::Hash128 compute_tree_token(const std::vector<FileEntry>& entries) {
    std::vector<const FileEntry*> sorted;
    sorted.reserve(entries.size());
    for (const auto& fe : entries) sorted.push_back(&fe);
    std::sort(sorted.begin(), sorted.end(),
              [](const FileEntry* a, const FileEntry* b) {
                  return a->rel_path < b->rel_path;
              });

    std::vector<u8> buf;
    buf.reserve(sorted.size() * 32);
    for (const FileEntry* fe : sorted) {
        buf.insert(buf.end(), fe->rel_path.begin(), fe->rel_path.end());
        u64 mt = fe->mtime_ns;
        u64 sz = fe->file_size;
        const u8* mp = reinterpret_cast<const u8*>(&mt);
        const u8* sp = reinterpret_cast<const u8*>(&sz);
        buf.insert(buf.end(), mp, mp + 8);
        buf.insert(buf.end(), sp, sp + 8);
    }
    return hash::xxh3_128(buf.data(), buf.size());
}

bool ClientSession::phase_pipeline_sync() {
    int num_conns = pool_.size();
    TcpSocket& conn0 = pool_.get(0);

    // Build file_id → entry lookup for fast dispatch
    std::unordered_map<u32, const FileEntry*> file_map;
    file_map.reserve(entries_.size());
    for (auto& fe : entries_) file_map[fe.file_id] = &fe;

    // ---- Create FileSender early and start parallel small-file pre-read ----
    // We do this BEFORE streaming the file list so that the pre-read overlaps
    // with FILE_LIST_ENTRY transmission and the subsequent WANT_FILE wait.
    // On Docker overlayfs, open()+read()+close() per file costs ~0.7 ms each;
    // 10 000 files × 0.7 ms = 7 s if done serially at send time.
    // With 8 parallel readers running while the network is busy, the overhead
    // is hidden behind the ~150–200 ms the client needs to process the list.
    FileSender sender(pool_, tui_state_, cfg_.use_compress, agreed_chunk_size_,
                      empty_delta_checksums(), empty_delta_block_sizes());
    if (agreed_caps_ & CAP_CHUNK_RESUME) {
        sender.set_chunk_resume(true);
    }
    if (cfg_.max_chunks > 0) {
        sender.set_max_chunks(cfg_.max_chunks);
    }

    // Collect candidate small files for background pre-read.
    // The thread is only started after we know it's a tree MISS (below),
    // so a tree cache HIT incurs zero wasted disk I/O.
    std::vector<const FileEntry*> preread_files;
    for (auto& fe : entries_) {
        if (fe.is_small) preread_files.push_back(&fe);
    }
    std::thread preread_th;  // started conditionally after HIT/MISS decision

    // ---- Step 1: Stream file tree to client on conn[0] ----
    // FIX 3: When CAP_TREE_CACHE is negotiated, embed the 16-byte tree_token
    // in the FILE_LIST_BEGIN payload. Then wait for the client to reply with
    // either MT_TREE_CACHE_HIT (skip sending entries) or MT_TREE_CACHE_MISS
    // (send entries as usual). This saves ~540 KB per incremental run when
    // nothing has changed.
    bool use_tree_cache = (agreed_caps_ & CAP_TREE_CACHE) != 0;
    bool tree_hit = false;

    if (use_tree_cache) {
        hash::Hash128 token = compute_tree_token(entries_);
        u8 token_bytes[16];
        hash::to_bytes(token, token_bytes);
        // Payload: [tree_token(16)][dir_id(16)] = 32 bytes
        u8 begin_payload[32];
        std::memcpy(begin_payload,      token_bytes,    16);
        std::memcpy(begin_payload + 16, cfg_.dir_id,    16);
        conn0.write_frame(MsgType::MT_FILE_LIST_BEGIN, 0, begin_payload, 32);

        // Wait for client's cache decision (HIT → skip entries, MISS → send them)
        FrameHeader resp_hdr{}; std::vector<u8> resp_pl;
        if (conn0.read_frame(resp_hdr, resp_pl)) {
            tree_hit = ((MsgType)resp_hdr.msg_type == MsgType::MT_TREE_CACHE_HIT);
        }
    } else {
        conn0.write_frame(MsgType::MT_FILE_LIST_BEGIN, 0, nullptr, 0);
    }

    if (!tree_hit) {
        // Start small-file pre-read in the background so it overlaps with the
        // FILE_LIST_ENTRY transmission below (~37 ms @ 100 mbit for 10k files)
        // and the subsequent WANT_FILE wait (~150 ms).  On Docker overlayfs
        // each open() costs ~40 µs; 8 threads reduce 10k files from ~400 ms
        // serial to ~55 ms parallel — hidden behind the network wait.
        if (!preread_files.empty()) {
            preread_th = std::thread([&]() {
                sender.prefill_small_cache(preread_files, 8);
            });
        }

        // Build the raw file-list blob (same layout as before: FileListEntry + path per file).
        // If CAP_COMPRESSED_TREE is agreed, compress the whole blob with zstd and send it
        // as a single MT_FILE_LIST_COMPRESSED frame (4-byte LE original_size prefix + zstd data).
        // This reduces 10 000-file list from ~520 KB to ~30-50 KB, saving ~8 s at 500 kbit.
        // Fallback: send individual MT_FILE_LIST_ENTRY frames as before.
        {
            // Serialise all entries into a flat buffer
            std::vector<u8> raw;
            raw.reserve(entries_.size() * 52);
            for (auto& fe : entries_) {
                FileListEntry fle{};
                fle.file_id   = fe.file_id;
                fle.file_size = fe.file_size;
                fle.mtime_ns  = fe.mtime_ns;
                fle.path_len  = (u16)fe.rel_path.size();
                fle.flags     = 0;
                if (fe.hash_computed)
                    hash::to_bytes(fe.xxh3_128, fle.xxh3_128);
                else
                    std::memset(fle.xxh3_128, 0, 16);
                FileListEntry encoded = fle;
                proto::encode_file_list_entry(encoded);
                const u8* ep = reinterpret_cast<const u8*>(&encoded);
                raw.insert(raw.end(), ep, ep + sizeof(FileListEntry));
                raw.insert(raw.end(), fe.rel_path.begin(), fe.rel_path.end());
            }

            bool use_compressed_tree = (agreed_caps_ & CAP_COMPRESSED_TREE) != 0;
            if (use_compressed_tree && !raw.empty()) {
                // Compress: prepend 4-byte LE original size so client can pre-allocate
                std::vector<u8> cdata = compress::compress_to_vec(raw.data(), raw.size());
                u32 orig_size = (u32)raw.size();
                std::vector<u8> payload(4 + cdata.size());
                std::memcpy(payload.data(), &orig_size, 4);  // little-endian on all targets
                std::memcpy(payload.data() + 4, cdata.data(), cdata.size());
                conn0.write_frame(MsgType::MT_FILE_LIST_COMPRESSED, 0,
                                  payload.data(), (u32)payload.size());
                LOG_INFO("Pipeline: sent compressed file tree (" +
                         std::to_string(entries_.size()) + " entries, " +
                         std::to_string(raw.size()) + " → " +
                         std::to_string(cdata.size()) + " bytes)");
            } else {
                // Legacy: individual frames + END
                TcpWriteBuffer wbuf(conn0);
                size_t off = 0;
                for (auto& fe : entries_) {
                    size_t entry_size = sizeof(FileListEntry) + fe.rel_path.size();
                    wbuf.write_frame(MsgType::MT_FILE_LIST_ENTRY, 0,
                                     raw.data() + off, (u32)entry_size);
                    off += entry_size;
                }
                wbuf.write_frame(MsgType::MT_FILE_LIST_END, 0, nullptr, 0);
                LOG_INFO("Pipeline: sent file tree (" +
                         std::to_string(entries_.size()) + " entries)");
            }
        }
    } else {
        LOG_INFO("Pipeline: tree cache HIT, skipped " +
                 std::to_string(entries_.size()) + " entries");
    }

    // ---- Step 2: Set up file dispatch ----
    // (FileSender was created above to allow early pre-read overlap)

    std::mutex                     want_mutex;
    std::condition_variable        want_cv;
    std::deque<u32>                want_queue;
    std::atomic<bool>              check_done{false};
    std::atomic<int>               round_robin{0};

    // FIX 2: small files are accumulated here and batch-sent after FILE_CHECK_DONE,
    // avoiding 1 bundle-message per file (1000 files → 1000 round-trips).
    std::vector<const FileEntry*>  pending_small;

    // dispatch_one: large files are sent immediately; small files are queued for
    // batch sending. Returns false only if the test-mode chunk limit is reached.
    auto dispatch_one = [&](u32 fid) -> bool {
        auto it = file_map.find(fid);
        if (it == file_map.end()) {
            LOG_WARN("Pipeline dispatch: unknown file_id=" + std::to_string(fid));
            return true;
        }
        const FileEntry& fe = *it->second;

        if (fe.is_small) {
            pending_small.push_back(&fe);
            return true;
        }

        // Large file: dispatch immediately on a data connection
        int ci;
        if (num_conns > 1) {
            int v = round_robin.fetch_add(1);
            ci = 1 + (v % (num_conns - 1));
        } else {
            ci = 0;
        }

        try {
            sender.init_file_tracking(fe.file_id, 1);
            sender.send_large_file(fe, /*resume_offset=*/0, ci);
            tui_state_.files_done.fetch_add(1);
        } catch (const std::runtime_error& e) {
            LOG_WARN("Pipeline dispatch stopped: " + std::string(e.what()));
            check_done.store(true);
            return false;
        }
        return true;
    };

    auto drain_queue = [&]() {
        std::deque<u32> local;
        {
            std::lock_guard<std::mutex> lk(want_mutex);
            local.swap(want_queue);
        }
        for (u32 fid : local) {
            if (!dispatch_one(fid)) break;
        }
    };

    // batch_send_small: split pending_small across data connections, one
    // send_bundle call per connection (respects MAX_BUNDLE_SIZE / MAX_BUNDLE_FILES).
    auto batch_send_small = [&]() {
        // Wait for the pre-read thread to finish before accessing the cache.
        // By the time FILE_CHECK_DONE arrives the pre-read is almost always
        // already complete (it ran in parallel with FILE_LIST_ENTRY + WANT_FILE).
        if (preread_th.joinable()) preread_th.join();

        if (pending_small.empty()) return;
        int num_data = (num_conns > 1) ? (num_conns - 1) : 1;

        // Partition files round-robin across data connections
        std::vector<std::vector<const FileEntry*>> batches((size_t)num_data);
        for (size_t i = 0; i < pending_small.size(); ++i)
            batches[i % (size_t)num_data].push_back(pending_small[i]);

        for (int d = 0; d < num_data; ++d) {
            if (batches[(size_t)d].empty()) continue;
            int ci = (num_conns > 1) ? (1 + d) : 0;

            // Send in MAX_BUNDLE_SIZE slices if the batch is very large
            const auto& batch = batches[(size_t)d];
            size_t start = 0;
            while (start < batch.size()) {
                std::vector<const FileEntry*> sub;
                u64 sub_bytes = 0;
                while (start < batch.size() &&
                       sub.size() < MAX_BUNDLE_FILES &&
                       sub_bytes + batch[start]->file_size <= MAX_BUNDLE_SIZE) {
                    sub_bytes += batch[start]->file_size;
                    sub.push_back(batch[start]);
                    ++start;
                }
                if (sub.empty()) { ++start; continue; } // 0-size file edge case
                try {
                    sender.send_bundle(sub, ci);
                    tui_state_.files_done.fetch_add((u32)sub.size());
                } catch (const std::exception& e) {
                    LOG_WARN("Batch bundle failed: " + std::string(e.what()));
                }
            }
        }
        LOG_INFO("Pipeline: batch-sent " + std::to_string(pending_small.size()) +
                 " small files");
    };

    // ---- N=1 fast path: single connection, must read all WANT_FILE before sending ----
    if (num_conns == 1) {
        std::vector<u32> want_list;
        for (;;) {
            FrameHeader hdr{}; std::vector<u8> pl;
            if (!conn0.read_frame(hdr, pl)) break;
            if ((MsgType)hdr.msg_type == MsgType::MT_FILE_CHECK_DONE) break;
            if ((MsgType)hdr.msg_type == MsgType::MT_WANT_FILE &&
                pl.size() >= sizeof(WantFileMsg))
            {
                WantFileMsg wmsg{};
                std::memcpy(&wmsg, pl.data(), sizeof(WantFileMsg));
                proto::decode_want_file_msg(wmsg);
                want_list.push_back(wmsg.file_id);
            }
        }
        for (u32 fid : want_list) {
            if (!dispatch_one(fid)) break;
        }
        batch_send_small();
        return true;
    }

    // ---- N>1: CV-based dispatcher runs in parallel with the WANT_FILE reader ----
    // FIX 1: replaced the old sleep_for(100ms) polling with a condition_variable
    // that wakes the dispatcher immediately whenever a WANT_FILE is enqueued.
    std::thread dispatcher([&]() {
        while (true) {
            {
                std::unique_lock<std::mutex> lk(want_mutex);
                want_cv.wait(lk, [&]{ return !want_queue.empty() || check_done.load(); });
            }
            drain_queue();
            if (check_done.load()) break;
        }
        drain_queue(); // catch any items that arrived with FILE_CHECK_DONE

        // FIX 2: all small files collected during the WANT_FILE phase are now
        // batch-sent — one bundle per data connection instead of 1000 individual bundles.
        batch_send_small();
    });

    // Main thread: read WANT_FILE messages on conn[0] until FILE_CHECK_DONE
    for (;;) {
        FrameHeader hdr{}; std::vector<u8> pl;
        if (!conn0.read_frame(hdr, pl)) break;
        if ((MsgType)hdr.msg_type == MsgType::MT_FILE_CHECK_DONE) break;
        if ((MsgType)hdr.msg_type == MsgType::MT_WANT_FILE &&
            pl.size() >= sizeof(WantFileMsg))
        {
            WantFileMsg wmsg{};
            std::memcpy(&wmsg, pl.data(), sizeof(WantFileMsg));
            proto::decode_want_file_msg(wmsg);
            {
                std::lock_guard<std::mutex> lk(want_mutex);
                want_queue.push_back(wmsg.file_id);
            }
            want_cv.notify_one(); // FIX 1: wake dispatcher immediately
        }
    }

    check_done.store(true);
    want_cv.notify_all(); // wake dispatcher for final drain
    dispatcher.join();

    // Safety: ensure preread thread is joined even if batch_send_small
    // was never called (e.g. no small files were WANT_FILEd).
    if (preread_th.joinable()) preread_th.join();

    LOG_INFO("Pipeline: all requested files dispatched");
    return true;
}

bool ClientSession::phase_done() {
    bool all_ok = true;
    for (int i = 0; i < pool_.size(); ++i) {
        try {
            TcpSocket& sock = pool_.get(i);
            sock.write_frame(MsgType::MT_SESSION_DONE, 0, nullptr, 0);
            // Drain until SESSION_DONE ACK arrives (client may send ACKs for
            // the final FILE_END before it processes our SESSION_DONE message).
            for (int attempts = 0; attempts < 8; ++attempts) {
                FrameHeader hdr{};
                std::vector<u8> payload;
                if (!sock.read_frame(hdr, payload)) break;
                if ((MsgType)hdr.msg_type == MsgType::MT_SESSION_DONE) break;
                // Any other message (e.g. FILE_END ACK) is silently discarded.
            }
        } catch (const std::exception& e) {
            LOG_WARN("phase_done conn " + std::to_string(i) + ": " + e.what());
            all_ok = false;
        }
    }
    return all_ok;
}
