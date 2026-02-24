// ============================================================
// dir_scanner.cpp -- Recursive directory scanner
// ============================================================

#include "dir_scanner.hpp"
#include "../common/file_io.hpp"
#include "../common/logger.hpp"
#include <algorithm>
#include <fstream>

namespace fs = std::filesystem;

DirScanner::DirScanner(const std::string& src_dir)
    : src_dir_(src_dir) {}

DirScanner::~DirScanner() {
    if (scan_thread_.joinable()) {
        scan_thread_.join();
    }
}

void DirScanner::start_async() {
    scan_thread_ = std::thread([this]{ scan_thread_fn(); });
}

std::vector<FileEntry> DirScanner::get_all() {
    if (scan_thread_.joinable()) {
        scan_thread_.join();
    }
    return results_;
}

FileEntry DirScanner::make_entry(const fs::path& abs_path, const fs::path& rel_path) {
    FileEntry fe;
    fe.file_id   = next_file_id_.fetch_add(1);
    fe.abs_path  = abs_path.string();
    fe.rel_path  = rel_path.generic_string(); // always use / separators
    fe.file_size = file_io::get_file_size(abs_path.string());
    fe.mtime_ns  = file_io::get_mtime_ns(abs_path.string());
    fe.is_small  = fe.file_size <= BUNDLE_THRESHOLD;
    // Hash is computed lazily in phase_file_list to avoid blocking server startup
    fe.xxh3_128     = {};
    fe.hash_computed = false;
    return fe;
}

void DirScanner::scan_thread_fn() {
    try {
        fs::path root(src_dir_);
        std::error_code ec;

        for (auto& entry : fs::recursive_directory_iterator(root,
             fs::directory_options::skip_permission_denied, ec))
        {
            if (entry.is_regular_file(ec)) {
                fs::path rel = fs::relative(entry.path(), root, ec);
                if (ec) continue;
                FileEntry fe = make_entry(entry.path(), rel);
                file_count_.fetch_add(1);
                total_bytes_.fetch_add(fe.file_size);

                std::lock_guard<std::mutex> lk(results_mutex_);
                results_.push_back(std::move(fe));
            }
        }
    } catch (const std::exception& e) {
        LOG_ERROR("DirScanner exception: " + std::string(e.what()));
    }

    done_.store(true);
    LOG_INFO("Scan complete: " + std::to_string(file_count_.load()) +
             " files, " + std::to_string(total_bytes_.load()) + " bytes");
}
