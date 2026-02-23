#pragma once

// ============================================================
// client_app.hpp -- FastCP client: connects to server,
//   receives files with retry-connect support
// ============================================================

#include "../common/platform.hpp"
#include "../common/socket.hpp"
#include "session_manager.hpp"
#include <string>
#include <atomic>

class ClientApp {
public:
    // num_conns: number of parallel TCP connections to request
    //            (sent in HANDSHAKE_REQ; server may override via ACK)
    ClientApp(const std::string& dst_dir,
              const std::string& server_ip,
              u16 server_port,
              int retry_secs = 30,
              int num_conns  = 4);
    ~ClientApp();

    // Connect to server (with retry), receive files, return.
    // Returns 0 on success, nonzero on error.
    int run();

    // Signal stop from a signal handler
    void stop();

private:
    std::string dst_dir_;
    std::string server_ip_;
    u16         server_port_;
    int         retry_secs_;
    int         num_conns_;      // requested parallel connections

    std::atomic<bool> stop_{false};
    SessionManager    session_mgr_;

    // Try to establish num_conns connections with retry backoff.
    // Returns true and fills sockets on success.
    bool connect_with_retry(int num_conns,
                            std::vector<TcpSocket>& sockets_out);
};
