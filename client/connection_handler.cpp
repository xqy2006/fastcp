// ============================================================
// connection_handler.cpp -- Per-connection state machine
// ============================================================

#include "connection_handler.hpp"
#include "../common/protocol_io.hpp"
#include "../common/logger.hpp"
#include "../common/utils.hpp"
#include "../common/file_io.hpp"
#include "../common/hash.hpp"
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
            if (!handle_file_list()) {
                LOG_ERROR("File list exchange failed");
                return;
            }
            if (!build_and_send_sync_plan()) {
                LOG_ERROR("Sync plan failed");
                return;
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

        // Check if file already exists at destination
        std::error_code ec;
        bool exists = fs::exists(abs_path, ec);
        u64 existing_size = exists ? file_io::get_file_size(abs_path.string()) : 0;

        // Check transfer index for partial progress
        u64 received = tidx.get_received(cfe.file_id);

        // Fast path: size mismatch -> FULL or PARTIAL
        if (!exists || existing_size != cfe.file_size) {
            if (received > 0 && received < cfe.file_size) {
                spe.action = (u8)SyncAction::PARTIAL;
                spe.resume_offset = received;
            } else {
                spe.action = (u8)SyncAction::FULL;
                spe.resume_offset = 0;
            }
        }
        // Size matches: compare content hash to determine if SKIP or FULL
        else {
            // Compute hash of local file and compare with server's hash
            bool content_matches = false;
            try {
                file_io::MmapReader reader(abs_path.string());
                if (reader.data() && reader.size() == existing_size) {
                    hash::Hash128 local_hash = hash::xxh3_128(reader.data(), reader.size());
                    content_matches = (std::memcmp(local_hash.data(), cfe.xxh3_128, 16) == 0);
                }
            } catch (...) {
                // If we can't read, treat as FULL
            }

            if (content_matches) {
                spe.action = (u8)SyncAction::SKIP;
                spe.resume_offset = 0;
            } else {
                spe.action = (u8)SyncAction::FULL;
                spe.resume_offset = 0;
            }
        }

        proto::encode_sync_plan_entry(spe);
        sock_.write_frame(MsgType::MT_SYNC_PLAN_ENTRY, 0, &spe, sizeof(spe));
    }

    sock_.write_frame(MsgType::MT_SYNC_PLAN_END, 0, nullptr, 0);

    LOG_INFO("Sync plan sent for " + std::to_string(file_list.size()) + " files");

    // ---- Delta sync: send block checksums for files that exist locally ----
    // The server will use these to skip unchanged blocks (rsync-style).
    // We send checksums for files that are FULL (content differs) or PARTIAL.
    // Small files (<= BUNDLE_THRESHOLD) are sent whole so no delta needed.
    // Block size = same as server's chunk_size (stored in session).

    bool use_delta = (session_->capabilities & CAP_DELTA) != 0;
    if (use_delta) {
        u32 block_size = session_->chunk_size;
        if (block_size == 0) block_size = DEFAULT_CHUNK_SIZE;

        for (auto& cfe : file_list) {
            // Only files larger than BUNDLE_THRESHOLD benefit from delta
            if (cfe.file_size <= BUNDLE_THRESHOLD) continue;

            // Determine action for this file (re-derive from what we sent)
            fs::path abs_path;
            try {
                abs_path = file_io::proto_to_fspath(session_->root_dir, cfe.rel_path);
            } catch (...) { continue; }

            std::error_code ec;
            bool exists = fs::exists(abs_path, ec);
            if (!exists) continue; // no local file to compare

            u64 existing_size = file_io::get_file_size(abs_path.string());
            if (existing_size == 0) continue; // nothing to diff

            // Compute block checksums for the local file
            try {
                file_io::MmapReader reader(abs_path.string());
                if (!reader.data()) continue;

                u64 local_size = reader.size();
                u32 num_blocks = (u32)((local_size + block_size - 1) / block_size);

                // Build payload: BlockChecksumMsg + num_blocks * BlockChecksumEntry
                size_t payload_size = sizeof(BlockChecksumMsg) +
                                      (size_t)num_blocks * sizeof(BlockChecksumEntry);
                std::vector<u8> payload(payload_size);

                BlockChecksumMsg hdr_msg{};
                hdr_msg.file_id     = cfe.file_id;
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

                // Mark this file as having delta checksums sent
                {
                    std::lock_guard<std::mutex> lk(session_->delta_sent_mutex);
                    session_->delta_sent[cfe.file_id] = true;
                }

                LOG_DEBUG("Sent " + std::to_string(num_blocks) +
                          " block checksums for " + cfe.rel_path);
            } catch (const std::exception& e) {
                LOG_WARN("Delta: failed to read " + cfe.rel_path + ": " + e.what());
            }
        }
    }

    // Always send MT_BLOCK_CHECKSUMS_END to signal completion of checksum phase
    // (even if we sent zero checksum frames, server needs to know we're done)
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
    // All connections share the same FileReceiver stored in the session
    {
        std::lock_guard<std::mutex> lk(session_->receiver_mutex);
        if (!session_->shared_receiver) {
            // First connection to reach transfer phase creates it
            auto r = std::make_shared<FileReceiver>(session_);
            session_->shared_receiver = r;
        }
        receiver_ = std::static_pointer_cast<FileReceiver>(session_->shared_receiver);
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
                bundle_files_left = 0; // reset; on_bundle_begin reads count
                break;
            case MsgType::MT_BUNDLE_ENTRY:
                if (!on_bundle_entry(payload, bundle_files_left)) {
                    // Log but continue
                }
                break;
            case MsgType::MT_BUNDLE_END:
                // Nothing to do
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

        // Clean up the transfer index: all files successfully received,
        // no need to keep resume state.
        if (session_->transfer_index) {
            session_->transfer_index->destroy();
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
