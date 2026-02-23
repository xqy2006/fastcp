// ============================================================
// connection_pool.cpp
// ============================================================

#include "connection_pool.hpp"
#include "../common/logger.hpp"

void ConnectionPool::init(const std::string& ip, u16 port, int num_conns) {
    sockets_.reserve((size_t)num_conns);
    slot_mutexes_ = std::vector<std::mutex>((size_t)num_conns);

    for (int i = 0; i < num_conns; ++i) {
        auto sock = std::make_unique<TcpSocket>();
        sock->connect(ip, port);
        sockets_.push_back(std::move(sock));
        LOG_DEBUG("Connection " + std::to_string(i) + " established to " +
                  ip + ":" + std::to_string(port));
    }
}

void ConnectionPool::init_from_accepted(std::vector<TcpSocket> sockets) {
    size_t n = sockets.size();
    sockets_.reserve(n);
    slot_mutexes_ = std::vector<std::mutex>(n);

    for (auto& s : sockets) {
        sockets_.push_back(std::make_unique<TcpSocket>(std::move(s)));
    }
}

TcpSocket& ConnectionPool::checkout(int index) {
    slot_mutexes_.at((size_t)index).lock();
    return *sockets_.at((size_t)index);
}

void ConnectionPool::checkin(int index) {
    slot_mutexes_.at((size_t)index).unlock();
}
