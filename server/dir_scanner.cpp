// ============================================================
// dir_scanner.cpp -- Recursive directory scanner
// ============================================================

#include "dir_scanner.hpp"
#include "../common/file_io.hpp"
#include "../common/logger.hpp"
#include <algorithm>
#include <fstream>
#ifndef _WIN32
#  include <sys/stat.h>
#endif

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
    fe.rel_path  = rel_path.generic_string();
    fe.file_size = file_io::get_file_size(abs_path.string());
    fe.mtime_ns  = file_io::get_mtime_ns(abs_path.string());
    fe.is_small  = fe.file_size <= BUNDLE_THRESHOLD;
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
            if (!entry.is_regular_file(ec)) continue;
            fs::path rel = fs::relative(entry.path(), root, ec);
            if (ec) continue;

            // Skip the .fastcp metadata directory (dir_id, treecache, etc.)
            if (!rel.empty() && rel.begin()->string() == ".fastcp") continue;

            FileEntry fe;
            fe.file_id  = next_file_id_.fetch_add(1);
            fe.abs_path = entry.path().string();
            fe.rel_path = rel.generic_string();

#ifdef _WIN32
            // directory_entry caches size and last_write_time from FindNextFile —
            // no extra GetFileAttributesEx needed.
            fe.file_size = entry.file_size(ec);
            if (ec) { ec.clear(); fe.file_size = 0; }
            {
                auto ftime = entry.last_write_time(ec);
                if (!ec) {
                    // MSVC file_time_type: 100-ns ticks since 1601-01-01
                    fe.mtime_ns = (u64)(ftime.time_since_epoch().count()
                                        - (long long)116444736000000000LL) * 100ULL;
                } else {
                    ec.clear();
                    fe.mtime_ns = 0;
                }
            }
#else
            // Use fstatat via the cached fd from the iterator to get size+mtime
            // in a single syscall, avoiding two separate stat() calls.
            struct ::stat st{};
            if (::stat(entry.path().c_str(), &st) == 0) {
                fe.file_size = (u64)st.st_size;
                fe.mtime_ns  = (u64)st.st_mtim.tv_sec * 1000000000ULL
                             + (u64)st.st_mtim.tv_nsec;
            } else {
                fe.file_size = 0;
                fe.mtime_ns  = 0;
            }
#endif
            fe.is_small      = fe.file_size <= BUNDLE_THRESHOLD;
            fe.xxh3_128      = {};
            fe.hash_computed = false;

            file_count_.fetch_add(1);
            total_bytes_.fetch_add(fe.file_size);

            std::lock_guard<std::mutex> lk(results_mutex_);
            results_.push_back(std::move(fe));
        }
    } catch (const std::exception& e) {
        LOG_ERROR("DirScanner exception: " + std::string(e.what()));
    }

    done_.store(true);
    LOG_INFO("Scan complete: " + std::to_string(file_count_.load()) +
             " files, " + std::to_string(total_bytes_.load()) + " bytes");
}

hash::Hash128 DirScanner::tree_token() const {
    // Build sorted list for stability across runs
    std::vector<const FileEntry*> sorted;
    sorted.reserve(results_.size());
    for (const auto& fe : results_) sorted.push_back(&fe);
    std::sort(sorted.begin(), sorted.end(),
              [](const FileEntry* a, const FileEntry* b) {
                  return a->rel_path < b->rel_path;
              });

    // Concatenate (rel_path + mtime_ns_le + file_size_le) for all entries
    std::vector<u8> buf;
    buf.reserve(sorted.size() * 32); // rough estimate
    for (const FileEntry* fe : sorted) {
        buf.insert(buf.end(), fe->rel_path.begin(), fe->rel_path.end());
        u64 mt = fe->mtime_ns;
        u64 sz = fe->file_size;
        const u8* mp = reinterpret_cast<const u8*>(&mt);
        const u8* sp = reinterpret_cast<const u8*>(&sz);
        buf.insert(buf.end(), mp, mp + 8);
        buf.insert(buf.end(), sp, sp + 8);
    }
    return hash::xxh3_128(buf.data(), buf.size());
}
