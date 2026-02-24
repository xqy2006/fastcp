#pragma once

// ============================================================
// tui.hpp -- ANSI TUI progress display
// ============================================================

#include "../common/platform.hpp"
#include <string>
#include <atomic>
#include <thread>
#include <mutex>
#include <vector>
#include <chrono>
#include <functional>

struct TuiState {
    std::atomic<u64> bytes_sent{0};
    std::atomic<u64> bytes_total{0};
    std::atomic<u32> files_done{0};
    std::atomic<u32> files_total{0};
    std::atomic<bool> active{false};
    std::string current_file;
    std::mutex current_file_mutex;
    std::string transfer_label{"Sent"};  // "Sent" for server, "Recv" for client
};

class Tui {
public:
    explicit Tui(TuiState& state);
    ~Tui();

    // Start background refresh thread (100ms interval)
    void start();

    // Stop and print final line
    void stop();

    // Render one frame to stdout
    void render();

    static bool is_tty();

private:
    TuiState& state_;
    std::thread thread_;
    std::atomic<bool> running_{false};

    // For speed calculation
    u64 last_bytes_{0};
    std::chrono::steady_clock::time_point last_time_;
    double smooth_speed_{0.0};

    // Track number of lines printed for cursor-up overwrite
    int lines_printed_{0};

    void clear_lines(int n);
    std::string build_progress_bar(double pct, int width) const;
    std::string build_line() const;
};
