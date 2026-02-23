#pragma once

// ============================================================
// utils.hpp -- Utility functions
// ============================================================

#include "platform.hpp"
#include <string>
#include <chrono>
#include <cstdint>
#include <sstream>
#include <iomanip>

namespace utils {

// Current time in milliseconds since epoch
inline u64 now_ms() {
    using namespace std::chrono;
    return (u64)duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()
    ).count();
}

// Current time in nanoseconds since epoch
inline u64 now_ns() {
    using namespace std::chrono;
    return (u64)duration_cast<nanoseconds>(
        system_clock::now().time_since_epoch()
    ).count();
}

// Format bytes as human-readable (e.g., "1.23 MB")
inline std::string format_bytes(u64 bytes) {
    if (bytes < 1024ULL) {
        return std::to_string(bytes) + " B";
    } else if (bytes < 1024ULL * 1024) {
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(2) << (double)bytes / 1024.0 << " KB";
        return ss.str();
    } else if (bytes < 1024ULL * 1024 * 1024) {
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(2) << (double)bytes / (1024.0 * 1024) << " MB";
        return ss.str();
    } else {
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(2) << (double)bytes / (1024.0 * 1024 * 1024) << " GB";
        return ss.str();
    }
}

// Format speed as "X.XX MB/s"
inline std::string format_speed(double bytes_per_sec) {
    std::ostringstream ss;
    if (bytes_per_sec < 1024.0) {
        ss << std::fixed << std::setprecision(1) << bytes_per_sec << " B/s";
    } else if (bytes_per_sec < 1024.0 * 1024) {
        ss << std::fixed << std::setprecision(2) << bytes_per_sec / 1024.0 << " KB/s";
    } else if (bytes_per_sec < 1024.0 * 1024 * 1024) {
        ss << std::fixed << std::setprecision(2) << bytes_per_sec / (1024.0 * 1024) << " MB/s";
    } else {
        ss << std::fixed << std::setprecision(2) << bytes_per_sec / (1024.0 * 1024 * 1024) << " GB/s";
    }
    return ss.str();
}

// Format duration as "1h 23m 45s" or "45s"
inline std::string format_duration_s(u64 seconds) {
    if (seconds < 60) return std::to_string(seconds) + "s";
    if (seconds < 3600) {
        return std::to_string(seconds / 60) + "m " + std::to_string(seconds % 60) + "s";
    }
    return std::to_string(seconds / 3600) + "h " +
           std::to_string((seconds % 3600) / 60) + "m " +
           std::to_string(seconds % 60) + "s";
}

// Validate IPv4 address string
inline bool validate_ip(const std::string& ip) {
    int a, b, c, d;
    if (sscanf(ip.c_str(), "%d.%d.%d.%d", &a, &b, &c, &d) != 4) return false;
    return (a >= 0 && a <= 255) && (b >= 0 && b <= 255) &&
           (c >= 0 && c <= 255) && (d >= 0 && d <= 255);
}

// Validate port number (1-65535)
inline bool validate_port(int port) {
    return port >= 1 && port <= 65535;
}

// Validate that a path is non-empty and doesn't contain null bytes
inline bool validate_path(const std::string& path) {
    if (path.empty()) return false;
    for (char c : path) {
        if (c == '\0') return false;
    }
    return true;
}

// Generate a 64-bit session ID (random)
inline u64 generate_session_id() {
    // Use high-resolution clock + some entropy
    using namespace std::chrono;
    u64 t = (u64)high_resolution_clock::now().time_since_epoch().count();
    // Mix with a static counter for uniqueness
    static u64 counter = 0;
    ++counter;
    // Simple hash mix
    t ^= counter * 0x9e3779b97f4a7c15ULL;
    t ^= (t >> 30) * 0xbf58476d1ce4e5b9ULL;
    t ^= (t >> 27) * 0x94d049bb133111ebULL;
    t ^= (t >> 31);
    return t == 0 ? 1 : t; // never return 0 (reserved for "unset")
}

// Progress percentage string
inline std::string format_percent(u64 done, u64 total) {
    if (total == 0) return "100%";
    double pct = (double)done / (double)total * 100.0;
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(1) << pct << "%";
    return ss.str();
}

// Clamp value
template<typename T>
inline T clamp(T val, T lo, T hi) {
    return val < lo ? lo : (val > hi ? hi : val);
}

} // namespace utils
