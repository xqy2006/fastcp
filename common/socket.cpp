// ============================================================
// socket.cpp -- TcpSocket implementation
// ============================================================

#include "socket.hpp"
#include "protocol_io.hpp"
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>
#include <algorithm>

#ifndef _WIN32
#  include <sys/uio.h>
#endif

// Buffer size for SO_SNDBUF / SO_RCVBUF = 4 MB
static constexpr int SOCKET_BUF_SIZE = 4 * 1024 * 1024;

TcpSocket::TcpSocket() {
    fd_ = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd_ == INVALID_SOCKET_VAL) {
        throw std::runtime_error("socket() failed: " + socket_error_str(last_socket_error()));
    }
    apply_socket_opts();
}

TcpSocket::TcpSocket(socket_t fd) : fd_(fd) {
    if (fd_ != INVALID_SOCKET_VAL) {
        apply_socket_opts();
    }
}

TcpSocket::~TcpSocket() {
    close();
}

TcpSocket::TcpSocket(TcpSocket&& o) noexcept : fd_(o.fd_) {
    o.fd_ = INVALID_SOCKET_VAL;
}

TcpSocket& TcpSocket::operator=(TcpSocket&& o) noexcept {
    if (this != &o) {
        close();
        fd_ = o.fd_;
        o.fd_ = INVALID_SOCKET_VAL;
    }
    return *this;
}

void TcpSocket::apply_socket_opts() {
    // SO_REUSEADDR
    int on = 1;
#ifdef _WIN32
    setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, (const char*)&on, sizeof(on));
#else
    setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
#endif
}

void TcpSocket::tune() {
    int nodelay = 1;
    int keepalive = 1;
    int sndbuf = SOCKET_BUF_SIZE;
    int rcvbuf = SOCKET_BUF_SIZE;

#ifdef _WIN32
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY,  (const char*)&nodelay,   sizeof(nodelay));
    setsockopt(fd_, SOL_SOCKET,  SO_KEEPALIVE, (const char*)&keepalive, sizeof(keepalive));
    setsockopt(fd_, SOL_SOCKET,  SO_SNDBUF,    (const char*)&sndbuf,    sizeof(sndbuf));
    setsockopt(fd_, SOL_SOCKET,  SO_RCVBUF,    (const char*)&rcvbuf,    sizeof(rcvbuf));
#else
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY,  &nodelay,   sizeof(nodelay));
    setsockopt(fd_, SOL_SOCKET,  SO_KEEPALIVE, &keepalive, sizeof(keepalive));
    setsockopt(fd_, SOL_SOCKET,  SO_SNDBUF,    &sndbuf,    sizeof(sndbuf));
    setsockopt(fd_, SOL_SOCKET,  SO_RCVBUF,    &rcvbuf,    sizeof(rcvbuf));
#  ifdef TCP_CORK
    int cork = 0;
    setsockopt(fd_, IPPROTO_TCP, TCP_CORK, &cork, sizeof(cork));
#  endif
#endif
}

void TcpSocket::connect(const std::string& ip, u16 port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
#ifdef _WIN32
    addr.sin_addr.s_addr = inet_addr(ip.c_str());
    if (addr.sin_addr.s_addr == INADDR_NONE) {
        throw std::runtime_error("Invalid IP address: " + ip);
    }
#else
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) != 1) {
        throw std::runtime_error("Invalid IP address: " + ip);
    }
#endif
    if (::connect(fd_, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR_VAL) {
        throw std::runtime_error("connect() failed: " + socket_error_str(last_socket_error()));
    }
    tune();
}

void TcpSocket::bind_and_listen(const std::string& ip, u16 port, int backlog) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (ip.empty() || ip == "0.0.0.0") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
#ifdef _WIN32
        addr.sin_addr.s_addr = inet_addr(ip.c_str());
#else
        inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);
#endif
    }
    if (::bind(fd_, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR_VAL) {
        throw std::runtime_error("bind() failed: " + socket_error_str(last_socket_error()));
    }
    if (::listen(fd_, backlog) == SOCKET_ERROR_VAL) {
        throw std::runtime_error("listen() failed: " + socket_error_str(last_socket_error()));
    }
}

TcpSocket TcpSocket::accept() {
    sockaddr_in peer{};
#ifdef _WIN32
    int peer_len = sizeof(peer);
#else
    socklen_t peer_len = sizeof(peer);
#endif
    socket_t client = ::accept(fd_, (sockaddr*)&peer, &peer_len);
    if (client == INVALID_SOCKET_VAL) {
        throw std::runtime_error("accept() failed: " + socket_error_str(last_socket_error()));
    }
    return TcpSocket(client);
}

void TcpSocket::send_all(const void* buf, size_t len) {
    const char* p = static_cast<const char*>(buf);
    size_t remaining = len;
    while (remaining > 0) {
#ifdef _WIN32
        int sent = ::send(fd_, p, (int)std::min(remaining, (size_t)INT_MAX), 0);
#else
        ssize_t sent = ::send(fd_, p, remaining, MSG_NOSIGNAL);
#endif
        if (sent <= 0) {
            if (sent == 0) {
                throw std::runtime_error("Connection closed during send");
            }
            int err = last_socket_error();
            if (would_block(err)) {
                // busy-wait (shouldn't happen in blocking mode)
                continue;
            }
            throw std::runtime_error("send() failed: " + socket_error_str(err));
        }
        p += sent;
        remaining -= static_cast<size_t>(sent);
    }
}

bool TcpSocket::recv_all(void* buf, size_t len) {
    char* p = static_cast<char*>(buf);
    size_t remaining = len;
    while (remaining > 0) {
#ifdef _WIN32
        int received = ::recv(fd_, p, (int)std::min(remaining, (size_t)INT_MAX), 0);
#else
        ssize_t received = ::recv(fd_, p, remaining, 0);
#endif
        if (received == 0) return false; // clean close
        if (received < 0) {
            int err = last_socket_error();
            if (would_block(err)) {
                // Timeout (SO_RCVTIMEO) - return false instead of busy-wait
                return false;
            }
            throw std::runtime_error("recv() failed: " + socket_error_str(err));
        }
        p += received;
        remaining -= static_cast<size_t>(received);
    }
    return true;
}

void TcpSocket::write_frame(MsgType type, u16 flags, const void* payload, u32 payload_len) {
    u8 hdr_buf[8];
    FrameHeader hdr;
    hdr.msg_type    = static_cast<u16>(type);
    hdr.flags       = flags;
    hdr.payload_len = payload_len;
    proto::encode_header(hdr, hdr_buf);

    if (payload_len == 0 || !payload) {
        send_all(hdr_buf, 8);
        return;
    }

#ifdef _WIN32
    // WSASend: merge header + payload into one syscall, handle partial sends
    const char* p0 = reinterpret_cast<const char*>(hdr_buf);
    const char* p1 = static_cast<const char*>(payload);
    size_t remaining0 = 8, remaining1 = payload_len;
    bool hdr_done = false;
    while (!hdr_done || remaining1 > 0) {
        WSABUF wb[2];
        int cnt = 0;
        if (!hdr_done) { wb[cnt].buf = const_cast<char*>(p0); wb[cnt].len = (ULONG)remaining0; ++cnt; }
        if (remaining1 > 0) { wb[cnt].buf = const_cast<char*>(p1); wb[cnt].len = (ULONG)std::min(remaining1, (size_t)INT_MAX); ++cnt; }
        DWORD sent = 0;
        int rc = WSASend(fd_, wb, cnt, &sent, 0, nullptr, nullptr);
        if (rc == SOCKET_ERROR) throw std::runtime_error("WSASend failed: " + socket_error_str(WSAGetLastError()));
        size_t n = sent;
        if (!hdr_done) {
            size_t take = std::min(n, remaining0);
            p0 += take; remaining0 -= take; n -= take;
            if (remaining0 == 0) hdr_done = true;
        }
        if (n > 0) { p1 += n; remaining1 -= n; }
    }
#else
    // writev: merge header + payload into one syscall, handle partial sends
    size_t total = 8 + payload_len;
    size_t sent_total = 0;
    while (sent_total < total) {
        // Rebuild iovec from remaining data
        struct iovec cur[2];
        int cur_cnt = 0;
        size_t skip = sent_total;
        for (int i = 0; i < 2; ++i) {
            size_t seg_len = (i == 0) ? 8 : (size_t)payload_len;
            const char* seg_base = (i == 0) ? reinterpret_cast<const char*>(hdr_buf)
                                             : static_cast<const char*>(payload);
            if (skip >= seg_len) { skip -= seg_len; continue; }
            cur[cur_cnt].iov_base = const_cast<char*>(seg_base + skip);
            cur[cur_cnt].iov_len  = seg_len - skip;
            skip = 0;
            ++cur_cnt;
        }
        ssize_t n = ::writev(fd_, cur, cur_cnt);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error("writev failed: " + socket_error_str(errno));
        }
        sent_total += n;
    }
#endif
}

bool TcpSocket::read_frame(FrameHeader& hdr, std::vector<u8>& payload_buf) {
    u8 hdr_buf[8];
    if (!recv_all(hdr_buf, 8)) return false;
    hdr = proto::decode_header(hdr_buf);
    if (hdr.payload_len > MAX_PAYLOAD_LEN) {
        throw std::runtime_error("Payload too large: " + std::to_string(hdr.payload_len));
    }
    payload_buf.resize(hdr.payload_len);
    if (hdr.payload_len > 0) {
        if (!recv_all(payload_buf.data(), hdr.payload_len)) return false;
    }
    return true;
}

void TcpSocket::close() {
    if (fd_ != INVALID_SOCKET_VAL) {
        CLOSE_SOCKET(fd_);
        fd_ = INVALID_SOCKET_VAL;
    }
}

std::string TcpSocket::peer_addr() const {
    sockaddr_in peer{};
#ifdef _WIN32
    int len = sizeof(peer);
#else
    socklen_t len = sizeof(peer);
#endif
    if (getpeername(fd_, (sockaddr*)&peer, &len) == 0) {
        char buf[INET_ADDRSTRLEN] = {0};
#ifdef _WIN32
        const char* s = inet_ntoa(peer.sin_addr);
        if (s) return std::string(s) + ":" + std::to_string(ntohs(peer.sin_port));
#else
        if (inet_ntop(AF_INET, &peer.sin_addr, buf, sizeof(buf))) {
            return std::string(buf) + ":" + std::to_string(ntohs(peer.sin_port));
        }
#endif
    }
    return "unknown";
}

void TcpSocket::set_recv_timeout_ms(int ms) {
#ifdef _WIN32
    DWORD timeout = (DWORD)ms;
    setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
#else
    struct timeval tv;
    tv.tv_sec  = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif
}
