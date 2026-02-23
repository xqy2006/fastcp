// ============================================================
// session_manager.cpp
// ============================================================

#include "session_manager.hpp"
#include "../common/utils.hpp"
#include "../common/file_io.hpp"
#include "../common/logger.hpp"
#include <filesystem>

namespace fs = std::filesystem;

std::shared_ptr<SessionInfo> SessionManager::create_session(
    int total_conns, u16 capabilities, u32 chunk_size)
{
    auto session = std::make_shared<SessionInfo>();
    session->session_id  = utils::generate_session_id();
    session->total_conns = total_conns;
    session->capabilities = capabilities;
    session->chunk_size  = chunk_size ? chunk_size : DEFAULT_CHUNK_SIZE;
    session->root_dir    = root_dir_;
    session->transfer_index = std::make_unique<TransferIndex>(root_dir_);
    session->start_time  = std::chrono::steady_clock::now();
    session->conn_count.store(1);

    std::lock_guard<std::mutex> lk(mutex_);
    sessions_[session->session_id] = session;
    LOG_INFO("Session created: " + std::to_string(session->session_id) +
             " conns=" + std::to_string(total_conns));
    return session;
}

std::shared_ptr<SessionInfo> SessionManager::get_session(u64 session_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = sessions_.find(session_id);
    if (it == sessions_.end()) return nullptr;
    it->second->conn_count.fetch_add(1);
    return it->second;
}

void SessionManager::gc_sessions() {
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lk(mutex_);
    for (auto it = sessions_.begin(); it != sessions_.end(); ) {
        auto& s = it->second;
        auto age = std::chrono::duration_cast<std::chrono::seconds>(
            now - s->start_time).count();
        // Remove done sessions or stale sessions (> 5 minutes)
        if (s->done.load() || age > 300) {
            LOG_INFO("Session GC: " + std::to_string(it->first));
            it = sessions_.erase(it);
        } else {
            ++it;
        }
    }
}
