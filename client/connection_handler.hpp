#pragma once

// ============================================================
// connection_handler.hpp -- Per-connection state machine
// ============================================================

#include "../common/platform.hpp"
#include "../common/socket.hpp"
#include "../common/protocol.hpp"
#include "session_manager.hpp"
#include "file_receiver.hpp"
#include "archive_receiver.hpp"
#include <memory>
#include <string>
#include <vector>

enum class HandlerState {
    HANDSHAKE,
    FILE_LIST,
    TRANSFER,
    DONE,
    HS_ERROR,
};

class ConnectionHandler {
public:
    // Server-initiated mode: server accepted the socket; handler reads handshake
    ConnectionHandler(TcpSocket socket,
                      SessionManager& session_mgr,
                      int conn_index);

    // Client-initiated mode: client sent handshake; session already established;
    // handler skips handshake and goes straight to file-list / transfer.
    ConnectionHandler(TcpSocket socket,
                      std::shared_ptr<SessionInfo> session,
                      int conn_index,
                      bool skip_handshake);

    // Run the connection loop (blocking, call in its own thread)
    void run();

private:
    TcpSocket           sock_;
    SessionManager*     session_mgr_{nullptr};  // null in skip_handshake mode
    int                 conn_index_;
    bool                skip_handshake_{false};
    HandlerState        state_{HandlerState::HANDSHAKE};

    std::shared_ptr<SessionInfo>   session_;
    std::shared_ptr<FileReceiver>  receiver_;
    std::shared_ptr<ArchiveReceiver> archive_receiver_;

    // Temporary accumulation of archive manifest entries (conn[0] only)
    ArchiveManifestHdr            pending_manifest_hdr_{};
    std::vector<ArchiveFileSlot>  pending_archive_slots_;

    // ---- State handlers ----
    bool handle_handshake();
    bool handle_file_list();
    bool build_and_send_sync_plan();
    bool handle_transfer_loop();

    // ---- Message handlers during TRANSFER ----
    bool on_file_meta(const std::vector<u8>& payload);
    bool on_file_chunk(const std::vector<u8>& payload);
    bool on_file_end(const std::vector<u8>& payload);
    bool on_bundle_begin(const std::vector<u8>& payload);
    bool on_bundle_entry(const std::vector<u8>& payload, u32& bundle_files_left);
    bool on_session_done();

    // ---- Archive message handlers ----
    bool on_archive_manifest_hdr(const std::vector<u8>& payload);
    bool on_archive_file_entry(const std::vector<u8>& payload);
    bool on_archive_manifest_end();
    bool on_archive_chunk(const std::vector<u8>& payload);
    bool on_archive_done();

    // ---- Chunk-level resume ----
    // Receives CHUNK_HASH_LIST from server, checks local partial file, replies with
    // FILE_CHUNK_REQUEST containing only the chunks the client still needs.
    bool on_chunk_hash_list(const std::vector<u8>& payload);

    // ---- Pipeline sync ----
    // Reads server's file tree (FILE_LIST_ENTRY stream), checks each file locally,
    // sends MT_WANT_FILE for each file that needs transfer, then sends
    // MT_FILE_CHECK_DONE when all files have been inspected.
    bool handle_pipeline_file_tree();

    void send_ack(u32 ref_id, u16 ref_type, u16 chunk_index, u32 status = 0);
    void send_nack(u32 ref_id, u16 ref_type, u16 chunk_index, u32 error_code);
    void send_error(const std::string& msg);
};
