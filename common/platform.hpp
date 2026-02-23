#pragma once

// ============================================================
// platform.hpp -- Cross-platform socket/OS abstraction
// ============================================================

#ifdef _WIN32
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  ifndef _WIN32_WINNT
#    define _WIN32_WINNT 0x0601
#  endif
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  include <windows.h>
#  pragma comment(lib, "ws2_32.lib")

   using socket_t = SOCKET;
#  define INVALID_SOCKET_VAL INVALID_SOCKET
#  define SOCKET_ERROR_VAL   SOCKET_ERROR
#  define CLOSE_SOCKET(s)    closesocket(s)

   inline int last_socket_error() { return WSAGetLastError(); }
   inline bool would_block(int err) { return err == WSAEWOULDBLOCK; }

#else // POSIX
#  include <sys/types.h>
#  include <sys/socket.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <arpa/inet.h>
#  include <unistd.h>
#  include <fcntl.h>
#  include <errno.h>
#  include <netdb.h>

   using socket_t = int;
#  define INVALID_SOCKET_VAL (-1)
#  define SOCKET_ERROR_VAL   (-1)
#  define CLOSE_SOCKET(s)    ::close(s)

   inline int last_socket_error() { return errno; }
   inline bool would_block(int err) { return err == EAGAIN || err == EWOULDBLOCK; }
#endif

#include <cstdint>
#include <cstddef>
#include <stdexcept>
#include <string>

// ---- Platform init/cleanup ----

namespace platform {

inline void init() {
#ifdef _WIN32
    WSADATA wsa;
    int rc = WSAStartup(MAKEWORD(2, 2), &wsa);
    if (rc != 0) {
        throw std::runtime_error("WSAStartup failed: " + std::to_string(rc));
    }
#endif
}

inline void cleanup() {
#ifdef _WIN32
    WSACleanup();
#endif
}

// RAII guard for Winsock
struct Guard {
    Guard()  { init(); }
    ~Guard() { cleanup(); }
};

} // namespace platform

// ---- Portable types ----
using u8  = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;
using i8  = int8_t;
using i16 = int16_t;
using i32 = int32_t;
using i64 = int64_t;
