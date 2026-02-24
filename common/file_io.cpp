// ============================================================
// file_io.cpp -- Memory-mapped file I/O implementation
// ============================================================

#include "file_io.hpp"
#include <vector>
#include <fstream>
#include <cstring>
#include <stdexcept>
#include <string>
#include <filesystem>

#ifndef _WIN32
#  include <sys/mman.h>
#  include <sys/stat.h>
#  include <fcntl.h>
#  include <unistd.h>
#  include <utime.h>
#  include <time.h>
#endif

namespace fs = std::filesystem;
using namespace file_io;

// ============================================================
// MmapReader
// ============================================================

MmapReader::MmapReader(const std::string& path) {
#ifdef _WIN32
    file_handle_ = CreateFileA(path.c_str(), GENERIC_READ, FILE_SHARE_READ,
                               nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL |
                               FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
    if (file_handle_ == INVALID_HANDLE_VALUE) {
        throw std::runtime_error("Cannot open file: " + path);
    }

    LARGE_INTEGER sz{};
    GetFileSizeEx(file_handle_, &sz);
    size_ = (u64)sz.QuadPart;

    if (size_ == 0) {
        // Empty file: no mapping needed
        data_ = nullptr;
        return;
    }

    map_handle_ = CreateFileMappingA(file_handle_, nullptr, PAGE_READONLY, 0, 0, nullptr);
    if (!map_handle_) {
        CloseHandle(file_handle_);
        throw std::runtime_error("CreateFileMapping failed: " + path);
    }

    data_ = static_cast<const char*>(MapViewOfFile(map_handle_, FILE_MAP_READ, 0, 0, 0));
    if (!data_) {
        CloseHandle(map_handle_);
        CloseHandle(file_handle_);
        throw std::runtime_error("MapViewOfFile failed: " + path);
    }
#else
    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ < 0) {
        throw std::runtime_error("Cannot open file: " + path);
    }

    struct stat st{};
    if (fstat(fd_, &st) != 0) {
        ::close(fd_);
        throw std::runtime_error("fstat failed: " + path);
    }
    size_ = (u64)st.st_size;

    if (size_ == 0) {
        data_ = nullptr;
        return;
    }

    void* p = mmap(nullptr, (size_t)size_, PROT_READ, MAP_SHARED, fd_, 0);
    if (p == MAP_FAILED) {
        ::close(fd_);
        throw std::runtime_error("mmap failed: " + path);
    }
    madvise(p, (size_t)size_, MADV_SEQUENTIAL);
    madvise(p, std::min((size_t)size_, (size_t)4*1024*1024), MADV_WILLNEED);
    data_ = static_cast<const char*>(p);
#endif
}

MmapReader::~MmapReader() {
    close();
}

void MmapReader::close() {
#ifdef _WIN32
    if (data_) { UnmapViewOfFile(data_); data_ = nullptr; }
    if (map_handle_) { CloseHandle(map_handle_); map_handle_ = nullptr; }
    if (file_handle_ != INVALID_HANDLE_VALUE) { CloseHandle(file_handle_); file_handle_ = INVALID_HANDLE_VALUE; }
#else
    if (data_ && size_ > 0) { munmap((void*)data_, (size_t)size_); data_ = nullptr; }
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
#endif
    size_ = 0;
}

// ============================================================
// MmapWriter
// ============================================================

MmapWriter::~MmapWriter() {
    if (is_open()) {
        try { close(); } catch (...) {}
    }
}

void MmapWriter::open(const std::string& file_path, u64 size, bool resume) {
    path_ = file_path;
    size_ = size;

    // Ensure parent directories exist
    ensure_parent_dirs(file_path);

#ifdef _WIN32
    // resume=true: open existing file without truncating
    // FILE_SHARE_READ: allow concurrent MmapReader to verify existing chunks
    DWORD creation = resume ? OPEN_EXISTING : CREATE_ALWAYS;
    file_handle_ = CreateFileA(file_path.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ,
                               nullptr, creation, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file_handle_ == INVALID_HANDLE_VALUE) {
        if (resume) {
            // Fallback to creating if file doesn't exist yet
            file_handle_ = CreateFileA(file_path.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ,
                                       nullptr, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
        }
        if (file_handle_ == INVALID_HANDLE_VALUE) {
            throw std::runtime_error("Cannot create file: " + file_path);
        }
    }

    // Preallocate to full size (no-op if already correct size in resume mode)
    LARGE_INTEGER li;
    li.QuadPart = (LONGLONG)size;
    SetFilePointerEx(file_handle_, li, nullptr, FILE_BEGIN);
    SetEndOfFile(file_handle_);

    if (size == 0) {
        data_ = nullptr;
        return;
    }

    DWORD hi = (DWORD)(size >> 32);
    DWORD lo = (DWORD)(size & 0xFFFFFFFF);
    map_handle_ = CreateFileMappingA(file_handle_, nullptr, PAGE_READWRITE, hi, lo, nullptr);
    if (!map_handle_) {
        CloseHandle(file_handle_);
        throw std::runtime_error("CreateFileMapping(write) failed: " + path_);
    }

    data_ = static_cast<char*>(MapViewOfFile(map_handle_, FILE_MAP_WRITE, 0, 0, 0));
    if (!data_) {
        CloseHandle(map_handle_);
        CloseHandle(file_handle_);
        throw std::runtime_error("MapViewOfFile(write) failed: " + path_);
    }
#else
    // resume=true: open existing without O_TRUNC so partial data is preserved
    int flags = resume ? (O_RDWR | O_CREAT) : (O_RDWR | O_CREAT | O_TRUNC);
    fd_ = ::open(file_path.c_str(), flags, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("Cannot create file: " + file_path);
    }

    if (size > 0) {
        // Ensure file is at full size (posix_fallocate preserves existing data)
        int rc = posix_fallocate(fd_, 0, (off_t)size);
        if (rc != 0) {
            ftruncate(fd_, (off_t)size);
        }

        void* p = mmap(nullptr, (size_t)size, PROT_WRITE, MAP_SHARED, fd_, 0);
        if (p == MAP_FAILED) {
            ::close(fd_);
            throw std::runtime_error("mmap(write) failed: " + path_);
        }
        data_ = static_cast<char*>(p);
    } else {
        data_ = nullptr;
    }
#endif
}

void MmapWriter::write_at(u64 offset, const void* data, size_t len) {
    if (!data_) return;
    if (offset + len > size_) {
        throw std::runtime_error("MmapWriter::write_at out of bounds");
    }
    std::memcpy(data_ + offset, data, len);
}

void MmapWriter::close(u64 mtime_ns) {
#ifdef _WIN32
    if (data_) { FlushViewOfFile(data_, 0); UnmapViewOfFile(data_); data_ = nullptr; }
    if (map_handle_) { CloseHandle(map_handle_); map_handle_ = nullptr; }
    if (file_handle_ != INVALID_HANDLE_VALUE) {
        if (mtime_ns > 0) {
            // Convert ns to FILETIME (100-ns intervals since 1601-01-01)
            // FILETIME epoch offset = 116444736000000000 * 100ns
            u64 ft_val = mtime_ns / 100 + 116444736000000000ULL;
            FILETIME ft;
            ft.dwLowDateTime  = (DWORD)(ft_val & 0xFFFFFFFF);
            ft.dwHighDateTime = (DWORD)(ft_val >> 32);
            SetFileTime(file_handle_, nullptr, nullptr, &ft);
        }
        CloseHandle(file_handle_);
        file_handle_ = INVALID_HANDLE_VALUE;
    }
#else
    if (data_ && size_ > 0) {
        msync(data_, (size_t)size_, MS_SYNC);
        munmap(data_, (size_t)size_);
        data_ = nullptr;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    if (mtime_ns > 0 && !path_.empty()) {
        set_mtime(path_, mtime_ns);
    }
#endif
    size_ = 0;
}

// ============================================================
// Utility functions
// ============================================================

void file_io::preallocate(const std::string& path, u64 size) {
#ifdef _WIN32
    HANDLE h = CreateFileA(path.c_str(), GENERIC_WRITE, 0, nullptr,
                           CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (h == INVALID_HANDLE_VALUE) return;
    LARGE_INTEGER li; li.QuadPart = (LONGLONG)size;
    SetFilePointerEx(h, li, nullptr, FILE_BEGIN);
    SetEndOfFile(h);
    CloseHandle(h);
#else
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return;
    posix_fallocate(fd, 0, (off_t)size);
    ::close(fd);
#endif
}

void file_io::set_mtime(const std::string& path, u64 mtime_ns) {
#ifdef _WIN32
    HANDLE h = CreateFileA(path.c_str(), FILE_WRITE_ATTRIBUTES, 0, nullptr,
                           OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (h == INVALID_HANDLE_VALUE) return;
    u64 ft_val = mtime_ns / 100 + 116444736000000000ULL;
    FILETIME ft;
    ft.dwLowDateTime  = (DWORD)(ft_val & 0xFFFFFFFF);
    ft.dwHighDateTime = (DWORD)(ft_val >> 32);
    SetFileTime(h, nullptr, nullptr, &ft);
    CloseHandle(h);
#else
    struct timespec ts[2];
    ts[0].tv_sec  = (time_t)(mtime_ns / 1000000000ULL);
    ts[0].tv_nsec = (long)(mtime_ns % 1000000000ULL);
    ts[1] = ts[0];
    utimensat(AT_FDCWD, path.c_str(), ts, 0);
#endif
}

fs::path file_io::proto_to_fspath(const fs::path& root_dir, const std::string& relative_path) {
    // Security: reject absolute paths and path traversal
    if (relative_path.empty()) {
        throw std::runtime_error("Empty relative path");
    }
    if (relative_path[0] == '/' || relative_path[0] == '\\') {
        throw std::runtime_error("Absolute path rejected: " + relative_path);
    }
    if (relative_path.find("..") != std::string::npos) {
        throw std::runtime_error("Path traversal rejected: " + relative_path);
    }

    // Normalize
    fs::path rel(relative_path);
    fs::path full = (root_dir / rel).lexically_normal();

    // Verify result is still under root_dir
    auto root_normal = root_dir.lexically_normal();
    auto full_str = full.string();
    auto root_str = root_normal.string();

    if (full_str.find(root_str) != 0) {
        throw std::runtime_error("Path escapes root directory: " + relative_path);
    }

    return full;
}

void file_io::ensure_parent_dirs(const std::string& path) {
    fs::path p(path);
    auto parent = p.parent_path();
    if (!parent.empty()) {
        fs::create_directories(parent);
    }
}

u64 file_io::get_file_size(const std::string& path) {
    std::error_code ec;
    auto sz = fs::file_size(path, ec);
    if (ec) return 0;
    return (u64)sz;
}

u64 file_io::get_mtime_ns(const std::string& path) {
#ifdef _WIN32
    WIN32_FILE_ATTRIBUTE_DATA info{};
    if (!GetFileAttributesExA(path.c_str(), GetFileExInfoStandard, &info)) return 0;
    // FILETIME: 100-ns intervals since 1601-01-01; convert to ns since Unix epoch
    u64 ft = ((u64)info.ftLastWriteTime.dwHighDateTime << 32) | info.ftLastWriteTime.dwLowDateTime;
    // Subtract Windows epoch offset (116444736000000000 * 100ns)
    if (ft < 116444736000000000ULL) return 0;
    return (ft - 116444736000000000ULL) * 100ULL;
#else
    struct stat st{};
    if (::stat(path.c_str(), &st) != 0) return 0;
#  if defined(__linux__)
    return (u64)st.st_mtim.tv_sec * 1000000000ULL + (u64)st.st_mtim.tv_nsec;
#  elif defined(__APPLE__)
    return (u64)st.st_mtimespec.tv_sec * 1000000000ULL + (u64)st.st_mtimespec.tv_nsec;
#  else
    return (u64)st.st_mtime * 1000000000ULL;
#  endif
#endif
}

std::vector<u8> file_io::read_small_file(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) return {};
    f.seekg(0, std::ios::end);
    size_t sz = (size_t)f.tellg();
    f.seekg(0, std::ios::beg);
    std::vector<u8> buf(sz);
    f.read((char*)buf.data(), sz);
    return buf;
}
