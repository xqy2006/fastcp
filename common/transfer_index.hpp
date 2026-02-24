#pragma once

// ============================================================
// transfer_index.hpp -- Persistent resume index
// ============================================================

#include "platform.hpp"
#include <string>
#include <unordered_map>
#include <mutex>
#include <fstream>
#include <sstream>

// Tracks how many bytes have been received for each file_id.
// Stored as a simple text file (one entry per line: "file_id received_bytes").
// Used by the server to support resume (PARTIAL transfers).

class TransferIndex {
public:
    explicit TransferIndex(const std::string& dir) {
        path_ = dir + "/.transfer_index";
        load();
    }

    // Get already-received bytes for file_id (0 if not found)
    u64 get_received(u32 file_id) const {
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = index_.find(file_id);
        return it != index_.end() ? it->second : 0;
    }

    // Update received bytes for file_id and persist immediately.
    // Frequent persistence ensures crash recovery can resume from a recent checkpoint.
    void set_received(u32 file_id, u64 bytes) {
        std::lock_guard<std::mutex> lk(mutex_);
        index_[file_id] = bytes;
        save_locked();
    }

    // Remove entry (file fully received)
    void remove(u32 file_id) {
        std::lock_guard<std::mutex> lk(mutex_);
        index_.erase(file_id);
        save_locked();
    }

    // Clear all entries and delete the index file from disk
    void destroy() {
        std::lock_guard<std::mutex> lk(mutex_);
        index_.clear();
        // Delete the file; ignore errors (file might not exist)
        std::remove(path_.c_str());
    }

    // Clear all entries
    void clear() {
        std::lock_guard<std::mutex> lk(mutex_);
        index_.clear();
        save_locked();
    }

    // Check if a file is fully known (used for SKIP decision)
    bool has_entry(u32 file_id) const {
        std::lock_guard<std::mutex> lk(mutex_);
        return index_.count(file_id) > 0;
    }

private:
    void load() {
        std::ifstream f(path_);
        if (!f) return;
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty() || line[0] == '#') continue;
            std::istringstream ss(line);
            u32 fid = 0; u64 rcv = 0;
            if (ss >> fid >> rcv) {
                index_[fid] = rcv;
            }
        }
    }

    void save_locked() {
        std::ofstream f(path_, std::ios::trunc);
        if (!f) return;
        f << "# fastcp transfer index\n";
        for (auto& [fid, rcv] : index_) {
            f << fid << " " << rcv << "\n";
        }
    }

    mutable std::mutex mutex_;
    std::unordered_map<u32, u64> index_;
    std::string path_;
};
