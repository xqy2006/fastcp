#pragma once

// ============================================================
// socket.hpp -- RAII TCP socket wrapper
// ============================================================

#include "platform.hpp"
#include "protocol.hpp"
#include <string>
#include <stdexcept>
#include <vector>
#include <chrono>

class TcpSocket {
public:
    TcpSocket();
    explicit TcpSocket(socket_t fd);
    ~TcpSocket();

    // Non-copyable
    TcpSocket(const TcpSocket&) = delete;
    TcpSocket& operator=(const TcpSocket&) = delete;

    // Movable
    TcpSocket(TcpSocket&& o) noexcept;
    TcpSocket& operator=(TcpSocket&& o) noexcept;

    // Client: connect to remote
    void connect(const std::string& ip, u16 port);

    // Server: bind + listen
    void bind_and_listen(const std::string& ip, u16 port, int backlog = 128);

    // Accept one connection (blocking)
    TcpSocket accept();

    // Send exactly 'len' bytes; throws on error
    void send_all(const void* buf, size_t len);

    // Receive exactly 'len' bytes; returns false on clean close
    bool recv_all(void* buf, size_t len);

    // Send a complete frame (header + payload)
    void write_frame(MsgType type, u16 flags, const void* payload, u32 payload_len);

    // Read next frame: fills header, resizes payload_buf and reads payload
    // Returns false on clean close (peer disconnected)
    bool read_frame(FrameHeader& hdr, std::vector<u8>& payload_buf);

    // Apply TCP performance tuning
    void tune();

    bool is_valid() const { return fd_ != INVALID_SOCKET_VAL; }
    socket_t native() const { return fd_; }

    void close();

    // Get peer address as string
    std::string peer_addr() const;

    // Set receive timeout in milliseconds (0 = infinite)
    void set_recv_timeout_ms(int ms);

private:
    socket_t fd_{INVALID_SOCKET_VAL};

    void apply_socket_opts();
};
