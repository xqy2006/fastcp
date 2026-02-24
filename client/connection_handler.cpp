// ============================================================
// connection_handler.cpp -- Per-connection state machine
// ============================================================

#include "connection_handler.hpp"
#include "../common/protocol_io.hpp"
#include "../common/logger.hpp"
#include "../common/utils.hpp"
#include "../common/file_io.hpp"
#include "../common/hash.hpp"
#include "../common/compress.hpp"
#include <cstring>
#include <vector>
#include <filesystem>
#include <chrono>

namespace fs = std::filesystem;

ConnectionHandler::ConnectionHandler(TcpSocket socket,
                                     SessionManager& session_mgr,
                                     int conn_index)
    : sock_(std::move(socket))
    , session_mgr_(&session_mgr)
    , conn_index_(conn_index)
{
    sock_.tune();
}

ConnectionHandler::ConnectionHandler(TcpSocket socket,
                                     std::shared_ptr<SessionInfo> session,
                                     int conn_index,
                                     bool /*skip_handshake*/)
    : sock_(std::move(socket))
    , session_mgr_(nullptr)
    , conn_index_(conn_index)
    , skip_handshake_(true)
    , session_(std::move(session))
{
    // Socket already tuned by caller
}

void ConnectionHandler::run() {
    try {
        if (!skip_handshake_) {
            // Server-accepted mode: read handshake from wire
            if (!handle_handshake()) {
                LOG_WARN("Handshake failed from " + sock_.peer_addr());
                return;
            }
        }
        // skip_handshake_ mode: session_ already set by constructor

        // Only connection 0 receives the file list and builds sync plan
        if (conn_index_ == 0) {
            bool use_pipeline = session_ &&
                                (session_->capabilities & CAP_PIPELINE_SYNC);
            if (use_pipeline) {
                // Pipeline mode: read file tree, check files locally, send WANT_FILE
                if (!handle_pipeline_file_tree()) {
                    LOG_ERROR("Pipeline file tree failed");
                    return;
                }
            } else {
                if (!handle_file_list()) {
                    LOG_ERROR("File list exchange failed");
                    return;
                }
                if (!build_and_send_sync_plan()) {
                    LOG_ERROR("Sync plan failed");
                    return;
                }
            }
        } else {
            // Secondary connections: wait for file list to be ready on session
            if (session_) {
                std::unique_lock<std::mutex> lk(session_->file_list_mutex);
                session_->file_list_cv.wait_for(lk, std::chrono::seconds(30),
                    [this]{ return session_->file_list_ready; });
            }
        }

        if (!handle_transfer_loop()) {
            LOG_WARN("Transfer loop ended with error on conn " +
                     std::to_string(conn_index_));
        }
    } catch (const std::exception& e) {
        LOG_ERROR("ConnectionHandler exception: " + std::string(e.what()));
    }
}

// ---- HANDSHAKE ----

bool ConnectionHandler::handle_handshake() {
    if (!session_mgr_) {
        LOG_ERROR("handle_handshake called in skip_handshake mode");
        return false;
    }
    FrameHeader hdr{};
    std::vector<u8> payload;
    if (!sock_.read_frame(hdr, payload)) return false;

    if ((MsgType)hdr.msg_type != MsgType::MT_HANDSHAKE_REQ) {
        send_error("Expected HANDSHAKE_REQ");
        return false;
    }
    if (payload.size() < sizeof(HandshakeReq)) {
        send_error("Short handshake");
        return false;
    }

    HandshakeReq req{};
    std::memcpy(&req, payload.data(), sizeof(HandshakeReq));
    proto::decode_handshake_req(req);

    if (!handshake_req_valid_magic(req)) {
        send_error("Bad magic");
        return false;
    }
    if (req.version != FASTCP_VERSION) {
        send_error("Version mismatch");
        return false;
    }

    conn_index_ = (int)req.conn_index;

    // Get or create session
    if (req.session_id == 0) {
        // First connection: create session
        session_ = session_mgr_->create_session(
            req.num_connections,
            req.capabilities,
            DEFAULT_CHUNK_SIZE);
    } else {
        session_ = session_mgr_->get_session(req.session_id);
        if (!session_) {
            send_error("Unknown session_id");
            return false;
        }
    }

    // Create receiver for this session if needed
    if (conn_index_ == 0) {
        receiver_ = std::make_shared<FileReceiver>(session_);
        // Store in session (simple: session owns the receiver for sharing)
        // We'll use a shared pointer approach via session userdata
    }

    // Send ACK
    HandshakeAck ack{};
    ack.session_id     = session_->session_id;
    ack.accepted_conns = (u16)session_->total_conns;
    ack.capabilities   = session_->capabilities;
    ack.chunk_size_kb  = (u32)(session_->chunk_size / 1024);
    proto::encode_handshake_ack(ack);

    sock_.write_frame(MsgType::MT_HANDSHAKE_ACK, 0, &ack, sizeof(ack));

    LOG_INFO("Handshake OK: session=" + std::to_string(session_->session_id) +
             " conn=" + std::to_string(conn_index_));
    return true;
}

// ---- FILE LIST ----

bool ConnectionHandler::handle_file_list() {
    FrameHeader hdr{};
    std::vector<u8> payload;

    // Expect FILE_LIST_BEGIN
    if (!sock_.read_frame(hdr, payload)) return false;
    if ((MsgType)hdr.msg_type != MsgType::MT_FILE_LIST_BEGIN) {
        send_error("Expected FILE_LIST_BEGIN");
        return false;
    }

    // Read entries until FILE_LIST_END
    for (;;) {
        if (!sock_.read_frame(hdr, payload)) return false;

        if ((MsgType)hdr.msg_type == MsgType::MT_FILE_LIST_END) {
            break;
        }

        if ((MsgType)hdr.msg_type != MsgType::MT_FILE_LIST_ENTRY) {
            send_error("Expected FILE_LIST_ENTRY");
            return false;
        }

        if (payload.size() < sizeof(FileListEntry)) continue;

        FileListEntry entry{};
        std::memcpy(&entry, payload.data(), sizeof(FileListEntry));
        proto::decode_file_list_entry(entry);

        // Extract path
        size_t path_offset = sizeof(FileListEntry);
        if (payload.size() < path_offset + entry.path_len) {
            LOG_WARN("Short FileListEntry payload");
            continue;
        }

        ClientFileEntry cfe;
        cfe.file_id   = entry.file_id;
        cfe.file_size = entry.file_size;
        cfe.mtime_ns  = entry.mtime_ns;
        cfe.flags     = entry.flags;
        std::memcpy(cfe.xxh3_128, entry.xxh3_128, 16);
        cfe.rel_path.assign(
            (char*)(payload.data() + path_offset), entry.path_len);

        session_->file_list.push_back(std::move(cfe));
    }

    session_->files_total.store((u32)session_->file_list.size());

    // Signal other connections that file list is ready
    {
        std::lock_guard<std::mutex> lk(session_->file_list_mutex);
        session_->file_list_ready = true;
    }
    session_->file_list_cv.notify_all();

    LOG_INFO("File list received: " + std::to_string(session_->file_list.size()) + " files");
    return true;
}

// ---- SYNC PLAN ----

bool ConnectionHandler::build_and_send_sync_plan() {
    auto& file_list = session_->file_list;
    auto& tidx = *session_->transfer_index;

    sock_.write_frame(MsgType::MT_SYNC_PLAN_BEGIN, 0, nullptr, 0);

    // Track which files qualify for delta sync:
    // size matches server AND mtime differs → content partially differs → delta useful
    struct DeltaCandidate { u32 file_id; fs::path abs_path; u64 file_size; };
    std::vector<DeltaCandidate> delta_candidates;

    for (auto& cfe : file_list) {
        fs::path abs_path;
        try {
            abs_path = file_io::proto_to_fspath(session_->root_dir, cfe.rel_path);
        } catch (...) {
            continue;
        }

        SyncPlanEntry spe{};
        spe.file_id = cfe.file_id;
        spe.reserved = 0;

        std::error_code ec;
        bool exists = fs::exists(abs_path, ec);
        u64 existing_size = exists ? file_io::get_file_size(abs_path.string()) : 0;
        u64 received = tidx.get_received(cfe.file_id);

        if (!exists) {
            // File missing entirely → full transfer
            spe.action = (u8)SyncAction::FULL;
            spe.resume_offset = 0;
        } else if (existing_size != cfe.file_size) {
            // Size mismatch → PARTIAL if transfer_index has progress, else FULL
            if (received > 0 && received < cfe.file_size) {
                spe.action = (u8)SyncAction::PARTIAL;
                spe.resume_offset = received;
            } else {
                spe.action = (u8)SyncAction::FULL;
                spe.resume_offset = 0;
            }
        } else {
            // Size matches → check mtime first (fast path)
            u64 local_mtime = file_io::get_mtime_ns(abs_path.string());
            if (local_mtime == cfe.mtime_ns) {
                // mtime matches → assume content identical (rsync fast path)
                spe.action = (u8)SyncAction::SKIP;
                spe.resume_offset = 0;
            } else {
                // mtime differs, size same → compare full hash to be sure
                bool content_matches = false;
                try {
                    file_io::MmapReader reader(abs_path.string());
                    if (reader.data()) {
                        hash::Hash128 local_hash = hash::xxh3_128(reader.data(), reader.size());
                        content_matches = (std::memcmp(local_hash.data(), cfe.xxh3_128, 16) == 0);
                    }
                } catch (...) {}

                if (content_matches) {
                    spe.action = (u8)SyncAction::SKIP;
                    spe.resume_offset = 0;
                } else {
                    spe.action = (u8)SyncAction::FULL;
                    spe.resume_offset = 0;
                    // size matches + content differs → ideal candidate for delta sync
                    if (cfe.file_size > BUNDLE_THRESHOLD) {
                        delta_candidates.push_back({cfe.file_id, abs_path, cfe.file_size});
                    }
                }
            }
        }

        proto::encode_sync_plan_entry(spe);
        sock_.write_frame(MsgType::MT_SYNC_PLAN_ENTRY, 0, &spe, sizeof(spe));
    }

    sock_.write_frame(MsgType::MT_SYNC_PLAN_END, 0, nullptr, 0);

    LOG_INFO("Sync plan sent for " + std::to_string(file_list.size()) + " files" +
             " (delta candidates: " + std::to_string(delta_candidates.size()) + ")");

    // ---- Delta sync: send block checksums ONLY for files where
    //      size matches server AND content differs (mtime-differ + hash-differ).
    //      These are the only files where rsync-style block patching is useful.
    //      Files with size mismatch (FULL/PARTIAL) or identical content (SKIP)
    //      do NOT benefit from block checksums.
    bool use_delta = (session_->capabilities & CAP_DELTA) != 0;
    if (use_delta && !delta_candidates.empty()) {
        u32 block_size = session_->chunk_size;
        if (block_size == 0) block_size = DEFAULT_CHUNK_SIZE;

        for (auto& cand : delta_candidates) {
            try {
                file_io::MmapReader reader(cand.abs_path.string());
                if (!reader.data()) continue;

                u64 local_size = reader.size();
                u32 num_blocks = (u32)((local_size + block_size - 1) / block_size);

                size_t payload_size = sizeof(BlockChecksumMsg) +
                                      (size_t)num_blocks * sizeof(BlockChecksumEntry);
                std::vector<u8> payload(payload_size);

                BlockChecksumMsg hdr_msg{};
                hdr_msg.file_id     = cand.file_id;
                hdr_msg.block_count = num_blocks;
                hdr_msg.block_size  = block_size;
                hdr_msg.reserved    = 0;
                BlockChecksumMsg encoded_hdr = hdr_msg;
                proto::encode_block_checksum_msg(encoded_hdr);
                std::memcpy(payload.data(), &encoded_hdr, sizeof(BlockChecksumMsg));

                for (u32 bi = 0; bi < num_blocks; ++bi) {
                    u64 offset = (u64)bi * block_size;
                    u32 len    = (u32)reader.chunk_len(offset, block_size);
                    const char* ptr = reader.chunk_ptr(offset);

                    BlockChecksumEntry entry{};
                    entry.block_index = bi;
                    entry.adler32     = hash::adler32(ptr, len);
                    entry.xxh3_32     = hash::xxh3_32(ptr, len);
                    BlockChecksumEntry encoded_entry = entry;
                    proto::encode_block_checksum_entry(encoded_entry);
                    std::memcpy(payload.data() + sizeof(BlockChecksumMsg) +
                                (size_t)bi * sizeof(BlockChecksumEntry),
                                &encoded_entry, sizeof(BlockChecksumEntry));
                }

                sock_.write_frame(MsgType::MT_BLOCK_CHECKSUMS, 0,
                                  payload.data(), (u32)payload.size());
                {
                    std::lock_guard<std::mutex> lk(session_->delta_sent_mutex);
                    session_->delta_sent[cand.file_id] = true;
                }
                LOG_DEBUG("Delta: sent " + std::to_string(num_blocks) +
                          " block checksums for file_id=" + std::to_string(cand.file_id));
            } catch (const std::exception& e) {
                LOG_WARN("Delta: failed to read file_id=" +
                         std::to_string(cand.file_id) + ": " + e.what());
            }
        }
    }

    sock_.write_frame(MsgType::MT_BLOCK_CHECKSUMS_END, 0, nullptr, 0);

    // Signal secondary connections that file list and sync plan are ready
    {
        std::lock_guard<std::mutex> lk(session_->file_list_mutex);
        session_->file_list_ready = true;
    }
    session_->file_list_cv.notify_all();

    return true;
}

// ---- TRANSFER LOOP ----

bool ConnectionHandler::handle_transfer_loop() {
    // Setup standard file receiver (shared across all connections)
    {
        std::lock_guard<std::mutex> lk(session_->receiver_mutex);
        if (!session_->shared_receiver) {
            auto r = std::make_shared<FileReceiver>(session_);
            session_->shared_receiver = r;
        }
        receiver_ = std::static_pointer_cast<FileReceiver>(session_->shared_receiver);
    }

    bool archive_mode = (session_->capabilities & CAP_VIRTUAL_ARCHIVE) != 0 &&
                        !(session_->capabilities & CAP_PIPELINE_SYNC);

    // Secondary connections in archive mode wait until conn[0] has set up the receiver
    if (archive_mode && conn_index_ != 0) {
        std::unique_lock<std::mutex> lk(session_->archive_receiver_mutex);
        session_->archive_receiver_cv.wait_for(lk, std::chrono::seconds(60),
            [this]{ return session_->shared_archive_receiver != nullptr; });
        if (!session_->shared_archive_receiver) {
            LOG_ERROR("conn[" + std::to_string(conn_index_) +
                      "]: timeout waiting for archive receiver");
            return false;
        }
        archive_receiver_ = std::static_pointer_cast<ArchiveReceiver>(
            session_->shared_archive_receiver);
    }

    FrameHeader hdr{};
    std::vector<u8> payload;
    u32 bundle_files_left = 0;

    for (;;) {
        if (!sock_.read_frame(hdr, payload)) {
            LOG_INFO("Connection closed (conn " + std::to_string(conn_index_) + ")");
            return true;
        }

        MsgType mt = (MsgType)hdr.msg_type;

        switch (mt) {
            case MsgType::MT_FILE_META:
                if (!on_file_meta(payload)) return false;
                break;
            case MsgType::MT_CHUNK_HASH_LIST:
                // Chunk-level resume: check local partial file and reply with needed chunks
                if (!on_chunk_hash_list(payload)) {
                    // Non-fatal: log and continue (server will resend all if no reply)
                }
                break;
            case MsgType::MT_FILE_CHUNK:
                if (!on_file_chunk(payload)) {
                    // NACK already sent inside, continue
                }
                break;
            case MsgType::MT_FILE_END:
                if (!on_file_end(payload)) {
                    // Hash mismatch; client should handle
                }
                break;
            case MsgType::MT_BUNDLE_BEGIN:
                if (!on_bundle_begin(payload)) return false;
                bundle_files_left = 0;
                break;
            case MsgType::MT_BUNDLE_ENTRY:
                if (!on_bundle_entry(payload, bundle_files_left)) {
                    // Log but continue
                }
                break;
            case MsgType::MT_BUNDLE_END:
                break;
            case MsgType::MT_ARCHIVE_MANIFEST_HDR:
                if (!on_archive_manifest_hdr(payload)) return false;
                break;
            case MsgType::MT_ARCHIVE_FILE_ENTRY:
                if (!on_archive_file_entry(payload)) return false;
                break;
            case MsgType::MT_ARCHIVE_MANIFEST_END:
                if (!on_archive_manifest_end()) return false;
                break;
            case MsgType::MT_ARCHIVE_CHUNK:
                if (!on_archive_chunk(payload)) {
                    // Log but continue
                }
                break;
            case MsgType::MT_ARCHIVE_DONE:
                on_archive_done();
                break;
            case MsgType::MT_SESSION_DONE:
                return on_session_done();
            case MsgType::MT_PING:
                sock_.write_frame(MsgType::MT_PONG, 0, nullptr, 0);
                break;
            default:
                LOG_WARN("Unknown message type: " + std::to_string(hdr.msg_type));
                break;
        }
    }
}

// ---- MESSAGE HANDLERS ----

bool ConnectionHandler::on_file_meta(const std::vector<u8>& payload) {
    if (payload.size() < sizeof(FileMeta)) return false;
    FileMeta meta{};
    std::memcpy(&meta, payload.data(), sizeof(FileMeta));
    proto::decode_file_meta(meta);

    std::string rel_path;
    if (payload.size() >= sizeof(FileMeta) + meta.path_len) {
        rel_path.assign((char*)(payload.data() + sizeof(FileMeta)), meta.path_len);
    }

    return receiver_->on_file_meta(meta, rel_path);
}

bool ConnectionHandler::on_file_chunk(const std::vector<u8>& payload) {
    if (payload.size() < sizeof(FileChunk)) return false;
    FileChunk chunk{};
    std::memcpy(&chunk, payload.data(), sizeof(FileChunk));
    proto::decode_file_chunk(chunk);

    const u8* data = payload.data() + sizeof(FileChunk);
    int result = receiver_->on_file_chunk(chunk, data);

    if (result == 0) {
        // Success: no ACK (client uses non-blocking NACK-only poll)
        return true;
    } else if (result > 0) {
        // Hash mismatch: NACK
        send_nack(chunk.file_id, 2, (u16)result, 0x01 /* hash mismatch */);
        return false;
    } else {
        // Internal error
        send_nack(chunk.file_id, 0, 0, 0xFF);
        return false;
    }
}

bool ConnectionHandler::on_file_end(const std::vector<u8>& payload) {
    if (payload.size() < sizeof(FileEnd)) return false;
    FileEnd end_msg{};
    std::memcpy(&end_msg, payload.data(), sizeof(FileEnd));
    proto::decode_file_end(end_msg);

    bool ok = receiver_->on_file_end(end_msg);
    send_ack(end_msg.file_id, 0, 0, ok ? 0 : 0x02 /* hash mismatch */);
    return ok;
}

bool ConnectionHandler::on_bundle_begin(const std::vector<u8>& payload) {
    if (payload.size() < sizeof(BundleBegin)) return false;
    BundleBegin bb{};
    std::memcpy(&bb, payload.data(), sizeof(BundleBegin));
    proto::decode_bundle_begin(bb);
    LOG_DEBUG("Bundle begin: id=" + std::to_string(bb.bundle_id) +
              " files=" + std::to_string(bb.file_count));
    return true;
}

bool ConnectionHandler::on_bundle_entry(const std::vector<u8>& payload,
                                         u32& /*bundle_files_left*/) {
    if (payload.size() < sizeof(BundleEntryHdr)) return false;
    BundleEntryHdr hdr_msg{};
    std::memcpy(&hdr_msg, payload.data(), sizeof(BundleEntryHdr));
    proto::decode_bundle_entry_hdr(hdr_msg);

    size_t path_offset = sizeof(BundleEntryHdr);
    if (payload.size() < path_offset + hdr_msg.path_len) return false;

    std::string rel_path((char*)(payload.data() + path_offset), hdr_msg.path_len);

    size_t data_offset = path_offset + hdr_msg.path_len;
    if (payload.size() < data_offset + hdr_msg.file_size) return false;

    const u8* data = payload.data() + data_offset;
    return receiver_->on_bundle_entry(hdr_msg, rel_path, data, hdr_msg.file_size);
}

bool ConnectionHandler::on_session_done() {
    LOG_INFO("SESSION_DONE received (conn " + std::to_string(conn_index_) + ")");

    int remaining = session_->total_conns - session_->conn_count.load();
    if (remaining <= 1) {
        // Last connection: mark session done
        session_->done.store(true);
        u64 elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - session_->start_time).count();
        double speed = elapsed_ms > 0 ?
            (double)session_->bytes_received.load() / (elapsed_ms / 1000.0) : 0.0;

        LOG_INFO("Session complete: files=" + std::to_string(session_->files_done.load()) +
                 " bytes=" + utils::format_bytes(session_->bytes_received.load()) +
                 " speed=" + utils::format_speed(speed));

        // Clean up the transfer index only if all files were fully received.
        // If some transfers were incomplete (e.g. server stopped mid-transfer),
        // preserve the index so the next session can resume from where it left off.
        if (session_->transfer_index) {
            bool all_done = (session_->files_done.load() >= session_->files_total.load()
                             && session_->files_total.load() > 0);
            if (all_done) {
                session_->transfer_index->destroy();
            }
        }
    }

    // Send final ACK
    sock_.write_frame(MsgType::MT_SESSION_DONE, 0, nullptr, 0);
    return true;
}

void ConnectionHandler::send_ack(u32 ref_id, u16 ref_type, u16 chunk_index, u32 status) {
    Ack ack{};
    ack.ref_id      = ref_id;
    ack.ref_type    = ref_type;
    ack.chunk_index = chunk_index;
    ack.status      = status;
    proto::encode_ack(ack);
    sock_.write_frame(MsgType::MT_ACK, 0, &ack, sizeof(ack));
}

void ConnectionHandler::send_nack(u32 ref_id, u16 ref_type, u16 chunk_index, u32 error_code) {
    NackMsg nack{};
    nack.ref_id      = ref_id;
    nack.ref_type    = ref_type;
    nack.chunk_index = chunk_index;
    nack.error_code  = error_code;
    proto::encode_nack(nack);
    sock_.write_frame(MsgType::MT_NACK, 0, &nack, sizeof(nack));
}

void ConnectionHandler::send_error(const std::string& msg) {
    sock_.write_frame(MsgType::MT_ERROR_MSG, 0, msg.data(), (u32)msg.size());
}

// ---- PIPELINE FILE TREE ----

bool ConnectionHandler::handle_pipeline_file_tree() {
    // ---- Step 1: Read server's file tree ----
    FrameHeader hdr{}; std::vector<u8> payload;
    if (!sock_.read_frame(hdr, payload)) return false;
    if ((MsgType)hdr.msg_type != MsgType::MT_FILE_LIST_BEGIN) {
        send_error("Expected FILE_LIST_BEGIN in pipeline mode");
        return false;
    }

    for (;;) {
        if (!sock_.read_frame(hdr, payload)) return false;
        if ((MsgType)hdr.msg_type == MsgType::MT_FILE_LIST_END) break;
        if ((MsgType)hdr.msg_type != MsgType::MT_FILE_LIST_ENTRY) continue;
        if (payload.size() < sizeof(FileListEntry)) continue;

        FileListEntry entry{};
        std::memcpy(&entry, payload.data(), sizeof(FileListEntry));
        proto::decode_file_list_entry(entry);

        size_t path_off = sizeof(FileListEntry);
        if (payload.size() < path_off + entry.path_len) continue;

        ClientFileEntry cfe;
        cfe.file_id   = entry.file_id;
        cfe.file_size = entry.file_size;
        cfe.mtime_ns  = entry.mtime_ns;
        cfe.flags     = entry.flags;
        std::memcpy(cfe.xxh3_128, entry.xxh3_128, 16);
        cfe.rel_path.assign((char*)(payload.data() + path_off), entry.path_len);
        session_->file_list.push_back(std::move(cfe));
    }

    session_->files_total.store((u32)session_->file_list.size());

    // Signal secondary connections that they may enter handle_transfer_loop().
    // Do this BEFORE local file checking so conn[1..N-1] are ready to receive
    // data as soon as the server starts dispatching.
    {
        std::lock_guard<std::mutex> lk(session_->file_list_mutex);
        session_->file_list_ready = true;
    }
    session_->file_list_cv.notify_all();

    LOG_INFO("Pipeline: file tree received (" +
             std::to_string(session_->file_list.size()) + " files)");

    // ---- Step 2: Check each file, send WANT_FILE for any that need transfer ----
    for (auto& cfe : session_->file_list) {
        fs::path abs_path;
        try {
            abs_path = file_io::proto_to_fspath(session_->root_dir, cfe.rel_path);
        } catch (...) {
            // Cannot determine local path → request the file
            WantFileMsg wmsg{};
            wmsg.file_id = cfe.file_id;
            wmsg.reason  = 0;
            WantFileMsg enc = wmsg;
            proto::encode_want_file_msg(enc);
            sock_.write_frame(MsgType::MT_WANT_FILE, 0, &enc, sizeof(enc));
            continue;
        }

        std::error_code ec;
        bool exists    = fs::exists(abs_path, ec);
        u64  loc_size  = exists ? file_io::get_file_size(abs_path.string()) : 0;

        if (!exists || loc_size != cfe.file_size) {
            // File missing or size mismatch → request full transfer
            WantFileMsg wmsg{};
            wmsg.file_id = cfe.file_id;
            wmsg.reason  = 0;
            WantFileMsg enc = wmsg;
            proto::encode_want_file_msg(enc);
            sock_.write_frame(MsgType::MT_WANT_FILE, 0, &enc, sizeof(enc));
        } else {
            // Size matches → check mtime (rsync fast path)
            u64 loc_mtime = file_io::get_mtime_ns(abs_path.string());
            if (loc_mtime == cfe.mtime_ns) {
                // Timestamps identical → assume content up-to-date
                session_->files_done.fetch_add(1);
            } else {
                // mtime differs → verify content hash if server provided one
                bool has_server_hash = false;
                for (int b = 0; b < 16; ++b) {
                    if (cfe.xxh3_128[b] != 0) { has_server_hash = true; break; }
                }

                bool content_ok = false;
                if (has_server_hash) {
                    try {
                        file_io::MmapReader reader(abs_path.string());
                        if (reader.data()) {
                            hash::Hash128 h =
                                hash::xxh3_128(reader.data(), reader.size());
                            content_ok =
                                (std::memcmp(h.data(), cfe.xxh3_128, 16) == 0);
                        }
                    } catch (...) {}
                }

                if (content_ok) {
                    session_->files_done.fetch_add(1); // content identical, skip
                } else {
                    WantFileMsg wmsg{};
                    wmsg.file_id = cfe.file_id;
                    wmsg.reason  = has_server_hash ? u8(2) : u8(1); // 2=hash_diff, 1=mtime_diff
                    WantFileMsg enc = wmsg;
                    proto::encode_want_file_msg(enc);
                    sock_.write_frame(MsgType::MT_WANT_FILE, 0, &enc, sizeof(enc));
                }
            }
        }
    }

    // ---- Step 3: Notify server that all files have been checked ----
    sock_.write_frame(MsgType::MT_FILE_CHECK_DONE, 0, nullptr, 0);
    LOG_INFO("Pipeline: FILE_CHECK_DONE sent");
    return true;
}

// ---- CHUNK-LEVEL RESUME ----

bool ConnectionHandler::on_chunk_hash_list(const std::vector<u8>& payload) {
    if (payload.size() < sizeof(ChunkHashListHdr)) return false;

    ChunkHashListHdr hdr{};
    std::memcpy(&hdr, payload.data(), sizeof(ChunkHashListHdr));
    proto::decode_chunk_hash_list_hdr(hdr);

    u32 chunk_count = hdr.chunk_count;
    size_t expected = sizeof(ChunkHashListHdr) + (size_t)chunk_count * sizeof(u32);
    if (payload.size() < expected) {
        LOG_WARN("on_chunk_hash_list: short payload for file_id=" +
                 std::to_string(hdr.file_id));
        return false;
    }

    // Decode server's per-chunk hashes (network byte order → host)
    std::vector<u32> server_hashes(chunk_count);
    const u8* hash_ptr = payload.data() + sizeof(ChunkHashListHdr);
    for (u32 i = 0; i < chunk_count; ++i) {
        u32 v;
        std::memcpy(&v, hash_ptr + i * sizeof(u32), sizeof(u32));
        server_hashes[i] = proto::ntoh32(v);
    }

    // Determine which chunks we still need by comparing with local partial file
    std::vector<u32> needed;
    if (receiver_) {
        needed = receiver_->get_needed_chunks(
            hdr.file_id, chunk_count, hdr.chunk_size, server_hashes.data());
    } else {
        // No receiver yet (shouldn't happen): request everything
        for (u32 i = 0; i < chunk_count; ++i) needed.push_back(i);
    }

    // Build and send FILE_CHUNK_REQUEST
    FileChunkRequestHdr req_hdr{};
    req_hdr.file_id      = hdr.file_id;
    req_hdr.needed_count = (u32)needed.size();

    std::vector<u8> req_payload(sizeof(FileChunkRequestHdr) + needed.size() * sizeof(u32));
    FileChunkRequestHdr encoded_req = req_hdr;
    proto::encode_file_chunk_request_hdr(encoded_req);
    std::memcpy(req_payload.data(), &encoded_req, sizeof(FileChunkRequestHdr));

    u8* idx_ptr = req_payload.data() + sizeof(FileChunkRequestHdr);
    for (size_t i = 0; i < needed.size(); ++i) {
        u32 v = proto::hton32(needed[i]);
        std::memcpy(idx_ptr + i * sizeof(u32), &v, sizeof(u32));
    }

    sock_.write_frame(MsgType::MT_FILE_CHUNK_REQUEST, 0,
                      req_payload.data(), (u32)req_payload.size());

    LOG_DEBUG("CHUNK_RESUME file_id=" + std::to_string(hdr.file_id) +
              ": need " + std::to_string(needed.size()) +
              "/" + std::to_string(chunk_count) + " chunks");
    return true;
}

// ---- ARCHIVE MESSAGE HANDLERS ----

bool ConnectionHandler::on_archive_manifest_hdr(const std::vector<u8>& payload) {
    if (payload.size() < sizeof(ArchiveManifestHdr)) return false;
    std::memcpy(&pending_manifest_hdr_, payload.data(), sizeof(ArchiveManifestHdr));
    proto::decode_archive_manifest_hdr(pending_manifest_hdr_);
    pending_archive_slots_.clear();
    pending_archive_slots_.reserve(pending_manifest_hdr_.total_files);
    LOG_DEBUG("ARCHIVE_MANIFEST_HDR: files=" +
              std::to_string(pending_manifest_hdr_.total_files) +
              " chunks=" + std::to_string(pending_manifest_hdr_.total_chunks));
    return true;
}

bool ConnectionHandler::on_archive_file_entry(const std::vector<u8>& payload) {
    if (payload.size() < sizeof(ArchiveFileEntry)) return false;
    ArchiveFileEntry fe{};
    std::memcpy(&fe, payload.data(), sizeof(ArchiveFileEntry));
    proto::decode_archive_file_entry(fe);

    size_t path_off = sizeof(ArchiveFileEntry);
    if (payload.size() < path_off + fe.path_len) {
        LOG_WARN("Short ARCHIVE_FILE_ENTRY payload");
        return false;
    }
    std::string rel_path((char*)(payload.data() + path_off), fe.path_len);

    fs::path abs_path;
    try {
        abs_path = file_io::proto_to_fspath(session_->root_dir, rel_path);
    } catch (const std::exception& e) {
        LOG_WARN("archive_file_entry: bad path " + rel_path + ": " + e.what());
        return false;
    }

    ArchiveFileSlot slot;
    slot.file_id        = fe.file_id;
    slot.virtual_offset = fe.virtual_offset;
    slot.file_size      = fe.file_size;
    slot.mtime_ns       = fe.mtime_ns;
    slot.abs_path       = abs_path.string();
    std::memcpy(slot.expected_hash, fe.xxh3_128, 16);

    pending_archive_slots_.push_back(std::move(slot));
    return true;
}

bool ConnectionHandler::on_archive_manifest_end() {
    // Only conn[0] processes the manifest and sends CHUNK_REQUEST
    if (conn_index_ != 0) return true;

    // Create shared archive receiver
    auto ar = std::make_shared<ArchiveReceiver>(session_);
    ar->init(std::move(pending_archive_slots_),
             pending_manifest_hdr_.chunk_size,
             pending_manifest_hdr_.total_virtual_size);
    pending_archive_slots_.clear();

    // Preallocate all target files
    ar->preallocate_all();

    // Store in session and signal secondary connections
    {
        std::lock_guard<std::mutex> lk(session_->archive_receiver_mutex);
        session_->shared_archive_receiver = ar;
    }
    session_->archive_receiver_cv.notify_all();
    archive_receiver_ = ar;

    // Build and send CHUNK_REQUEST
    std::vector<u32> needed = ar->build_needed_chunks();

    ChunkRequestHdr req_hdr{};
    req_hdr.needed_count = (u32)needed.size();

    std::vector<u8> payload(sizeof(ChunkRequestHdr) + needed.size() * sizeof(u32));
    ChunkRequestHdr encoded_hdr = req_hdr;
    proto::encode_chunk_request_hdr(encoded_hdr);
    std::memcpy(payload.data(), &encoded_hdr, sizeof(ChunkRequestHdr));

    u8* id_ptr = payload.data() + sizeof(ChunkRequestHdr);
    for (size_t i = 0; i < needed.size(); ++i) {
        u32 cid = proto::hton32(needed[i]);
        std::memcpy(id_ptr + i * sizeof(u32), &cid, sizeof(u32));
    }

    sock_.write_frame(MsgType::MT_CHUNK_REQUEST, 0,
                      payload.data(), (u32)payload.size());

    LOG_INFO("CHUNK_REQUEST sent: " + std::to_string(needed.size()) + " chunks");
    return true;
}

bool ConnectionHandler::on_archive_chunk(const std::vector<u8>& payload) {
    if (!archive_receiver_) {
        // Might arrive before archive_receiver is set on conn[0] (shouldn't happen)
        LOG_WARN("on_archive_chunk: archive_receiver not ready (conn " +
                 std::to_string(conn_index_) + ")");
        return false;
    }
    if (payload.size() < sizeof(ArchiveChunkHdr)) return false;

    ArchiveChunkHdr hdr{};
    std::memcpy(&hdr, payload.data(), sizeof(ArchiveChunkHdr));
    proto::decode_archive_chunk_hdr(hdr);

    const u8* data_ptr = payload.data() + sizeof(ArchiveChunkHdr);
    u32       data_len = hdr.data_len;

    // Decompress if needed
    std::vector<u8> decomp_buf;
    if (hdr.compress_flag == 1 && hdr.raw_len > 0) {
        try {
            decomp_buf = compress::decompress_to_vec(data_ptr, data_len, hdr.raw_len);
            data_ptr = decomp_buf.data();
            data_len = hdr.raw_len;
        } catch (const std::exception& e) {
            LOG_WARN("on_archive_chunk: decompress failed for chunk " +
                     std::to_string(hdr.chunk_id) + ": " + e.what());
            return false;
        }
    }

    // Adjust hdr.data_len to raw_len so receiver sees the raw size
    ArchiveChunkHdr raw_hdr = hdr;
    raw_hdr.data_len = data_len;

    archive_receiver_->on_archive_chunk(raw_hdr, data_ptr);
    return true;
}

bool ConnectionHandler::on_archive_done() {
    int count = session_->archive_done_count.fetch_add(1) + 1;
    LOG_DEBUG("ARCHIVE_DONE received on conn " + std::to_string(conn_index_) +
              " (" + std::to_string(count) + "/" +
              std::to_string(session_->total_conns) + ")");
    // Session completion is handled by the subsequent MT_SESSION_DONE
    return true;
}
