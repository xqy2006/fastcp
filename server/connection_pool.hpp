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

    // Get the i-th connection (0-indexed)
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

    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

private:
    std::vector<std::unique_ptr<TcpSocket>> sockets_;
    std::vector<std::mutex> slot_mutexes_;
    std::atomic<int> rr_counter_{0};
};
