#pragma once

// ============================================================
// write_buffer.hpp -- Application-level write buffer for TcpSocket
//
// Coalesces many small write_frame() calls into a few large send()
// syscalls. Especially useful when streaming thousands of small frames
// (FILE_LIST_ENTRY, BUNDLE_ENTRY, WANT_FILE, etc.) where the per-frame
// syscall cost (10–30 µs each with TCP_NODELAY) dominates latency.
//
// Usage:
//   {
//       TcpWriteBuffer wbuf(sock);
//       for (auto& fe : entries)
//           wbuf.write_frame(MsgType::MT_FILE_LIST_ENTRY, 0, ...);
//       wbuf.write_frame(MsgType::MT_FILE_LIST_END, 0, nullptr, 0);
//   } // destructor flushes automatically
//
// Thread safety: NOT thread-safe; use one buffer per thread/connection.
// ============================================================

#include "socket.hpp"
#include "protocol_io.hpp"
#include <vector>

class TcpWriteBuffer {
public:
    // Flush (and send) when the internal buffer reaches this size.
    // 256 KB is large enough to amortise syscall overhead while still
    // fitting comfortably in L2/L3 cache.
    static constexpr size_t DEFAULT_THRESHOLD = 256 * 1024;

    explicit TcpWriteBuffer(TcpSocket& sock,
                            size_t threshold = DEFAULT_THRESHOLD)
        : sock_(sock), threshold_(threshold)
    {
        buf_.reserve(threshold + 65536);  // avoid re-alloc for one over-sized frame
    }

    ~TcpWriteBuffer() { flush(); }

    // Non-copyable, non-movable (holds a reference to TcpSocket)
    TcpWriteBuffer(const TcpWriteBuffer&) = delete;
    TcpWriteBuffer& operator=(const TcpWriteBuffer&) = delete;

    // Enqueue a frame.  Flushes automatically when buffer fills up.
    void write_frame(MsgType type, u16 flags, const void* data, u32 len) {
        u8 hdr_buf[8];
        FrameHeader hdr;
        hdr.msg_type    = static_cast<u16>(type);
        hdr.flags       = flags;
        hdr.payload_len = len;
        proto::encode_header(hdr, hdr_buf);

        buf_.insert(buf_.end(), hdr_buf, hdr_buf + 8);
        if (data && len > 0) {
            buf_.insert(buf_.end(),
                        static_cast<const u8*>(data),
                        static_cast<const u8*>(data) + len);
        }

        if (buf_.size() >= threshold_) flush();
    }

    // Send all buffered data to the socket in one or a few syscalls.
    void flush() {
        if (!buf_.empty()) {
            sock_.send_all(buf_.data(), buf_.size());
            buf_.clear();
        }
    }

private:
    TcpSocket&      sock_;
    size_t          threshold_;
    std::vector<u8> buf_;
};
