// ============================================================
// server_app.cpp -- FastCP server daemon implementation
// ============================================================

#include "server_app.hpp"
#include "archive_builder.hpp"
#include "../common/protocol_io.hpp"
#include "../common/logger.hpp"
#include "../common/utils.hpp"
#include <thread>
#include <algorithm>
#include <cstring>
#include <vector>
#include <chrono>
#include <iostream>

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
    // Scan source directory once at startup; entries reused for every client
    LOG_INFO("Scanning source directory: " + config_.src_dir);
    DirScanner scanner(config_.src_dir);
    scanner.start_async();
    {
        std::lock_guard<std::mutex> lk(entries_mutex_);
        entries_     = scanner.get_all();
        total_bytes_ = scanner.total_bytes();
    }

    if (entries_.empty()) {
        LOG_WARN("Source directory is empty.");
    } else {
        LOG_INFO("Ready: " + std::to_string(entries_.size()) +
                 " files (" + utils::format_bytes(total_bytes_) + ")");
    }

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

        u16 server_caps = CAP_COMPRESS | CAP_RESUME | CAP_BUNDLE | CAP_DELTA | CAP_VIRTUAL_ARCHIVE;
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

    SyncPlanMap plan;
    if (!phase_file_list(plan)) {
        LOG_ERROR("File list exchange failed");
        tui_->stop();
        return 1;
    }

    if (agreed_caps_ & CAP_VIRTUAL_ARCHIVE) {
        if (!phase_transfer_archive(plan)) {
            LOG_WARN("Archive transfer completed with some errors");
        }
    } else {
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

    // Send file list
    sock.write_frame(MsgType::MT_FILE_LIST_BEGIN, 0, nullptr, 0);

    for (auto& fe : entries_) {
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

    TcpSocket& sock0 = pool_.get(0);

    // 3. Send manifest on conn[0]
    {
        ArchiveManifestHdr hdr_msg{};
        hdr_msg.total_virtual_size = archive.total_size();
        hdr_msg.total_files        = (u32)archive.files().size();
        hdr_msg.chunk_size         = archive.chunk_size();
        hdr_msg.total_chunks       = (u32)archive.chunks().size();
        ArchiveManifestHdr encoded = hdr_msg;
        proto::encode_archive_manifest_hdr(encoded);
        sock0.write_frame(MsgType::MT_ARCHIVE_MANIFEST_HDR, 0,
                          &encoded, sizeof(encoded));
    }

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

        sock0.write_frame(MsgType::MT_ARCHIVE_FILE_ENTRY, 0,
                          payload.data(), (u32)payload.size());
    }

    sock0.write_frame(MsgType::MT_ARCHIVE_MANIFEST_END, 0, nullptr, 0);

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

    // Update TUI: count files sent
    tui_state_.files_done.fetch_add((u32)archive.files().size());

    return !any_failed.load();
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
