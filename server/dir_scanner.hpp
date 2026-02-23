#pragma once

// ============================================================
// dir_scanner.hpp -- Recursive directory scanner
// ============================================================

#include "../common/platform.hpp"
#include "../common/protocol.hpp"
#include "../common/hash.hpp"
#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <functional>
#include <filesystem>

// One file entry from directory scan
struct FileEntry {
    u32  file_id;       // assigned sequentially
    std::string rel_path;   // relative path from source root
    std::string abs_path;   // absolute path
    u64  file_size;
    u64  mtime_ns;
    hash::Hash128 xxh3_128; // computed by scanner
    bool is_small;      // size <= BUNDLE_THRESHOLD
};

// Thread-safe queue for scan results
template<typename T>
class SafeQueue {
public:
    void push(T item) {
        std::lock_guard<std::mutex> lk(mutex_);
        queue_.push(std::move(item));
        cv_.notify_one();
    }

    // Returns false if stopped and queue is empty
    bool pop(T& out, bool& stopped) {
        std::unique_lock<std::mutex> lk(mutex_);
        cv_.wait(lk, [this, &stopped] { return !queue_.empty() || stopped; });
        if (queue_.empty()) return false;
        out = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    void notify_stop() {
        cv_.notify_all();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return queue_.empty();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return queue_.size();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<T> queue_;
};

class DirScanner {
public:
    explicit DirScanner(const std::string& src_dir);
    ~DirScanner();

    // Start scanning in background; calls callback for each entry
    // (or use get_all() to collect them all)
    void start_async();

    // Wait for scanning to complete; returns all entries
    std::vector<FileEntry> get_all();

    // Check if scanning is done
    bool is_done() const { return done_.load(); }

    u32 total_files() const { return file_count_.load(); }
    u64 total_bytes() const { return total_bytes_.load(); }

private:
    std::string src_dir_;
    std::atomic<bool> done_{false};
    std::atomic<u32> file_count_{0};
    std::atomic<u64> total_bytes_{0};
    std::atomic<u32> next_file_id_{1};

    SafeQueue<FileEntry> result_queue_;
    std::thread scan_thread_;
    std::vector<FileEntry> results_;
    std::mutex results_mutex_;

    void scan_thread_fn();
    FileEntry make_entry(const std::filesystem::path& abs_path,
                         const std::filesystem::path& rel_path);
};
