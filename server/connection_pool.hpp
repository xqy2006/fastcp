#pragma once

// ============================================================
// connection_pool.hpp -- Pool of N TCP connections
// ============================================================

#include "../common/platform.hpp"
#include "../common/socket.hpp"
#include <vector>
#include <mutex>
#include <condition_variable>
#include <stdexcept>
#include <memory>
#include <string>
#include <atomic>

class ConnectionPool {
public:
    ConnectionPool() = default;
    ~ConnectionPool() = default;

    // Mode A (sender/client role): actively connect to remote server
    void init(const std::string& ip, u16 port, int num_conns);

    // Mode B (server role): wrap already-accepted sockets
    void init_from_accepted(std::vector<TcpSocket> sockets);

    // Get number of connections
    int size() const { return (int)sockets_.size(); }

    // Get the i-th connection (0-indexed) - UNSAFE, use only when single-threaded
    TcpSocket& get(int index) {
        return *sockets_.at((size_t)index);
    }

    // Round-robin: get next connection index
    int next_index() {
        return rr_counter_++ % (int)sockets_.size();
    }

    // Checkout connection by index (acquire lock on that slot)
    TcpSocket& checkout(int index);
    void checkin(int index);

    // RAII guard for checked-out connection
    class Guard {
    public:
        Guard(ConnectionPool& pool, int index) : pool_(pool), index_(index) {
            sock_ = &pool_.checkout(index_);
        }
        ~Guard() { if (index_ >= 0) pool_.checkin(index_); }

        Guard(Guard&& other) noexcept
            : pool_(other.pool_), index_(other.index_), sock_(other.sock_) {
            other.index_ = -1;
        }
        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;

        TcpSocket& socket() { return *sock_; }
        TcpSocket* operator->() { return sock_; }

    private:
        ConnectionPool& pool_;
        int index_;
        TcpSocket* sock_;
    };

    // Convenience: get a RAII guard
    Guard guard(int index) { return Guard(*this, index); }

    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

private:
    std::vector<std::unique_ptr<TcpSocket>> sockets_;
    std::vector<std::mutex> slot_mutexes_;
    std::atomic<int> rr_counter_{0};
};
