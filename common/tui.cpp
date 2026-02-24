// ============================================================
// tui.cpp -- ANSI progress display implementation
// ============================================================

#include "tui.hpp"
#include "../common/utils.hpp"
#include <iostream>
#include <iomanip>
#include <sstream>
#include <cmath>
#include <cstdio>

#ifdef _WIN32
#  include <windows.h>
#  include <io.h>
#  define ISATTY _isatty
#  define FILENO _fileno
#else
#  include <unistd.h>
#  define ISATTY isatty
#  define FILENO fileno
#endif

bool Tui::is_tty() {
    return ISATTY(FILENO(stdout)) != 0;
}

Tui::Tui(TuiState& state)
    : state_(state)
    , last_time_(std::chrono::steady_clock::now())
{}

Tui::~Tui() {
    stop();
}

void Tui::start() {
    if (running_.exchange(true)) return;
    thread_ = std::thread([this] {
        while (running_.load()) {
            render();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });
}

void Tui::stop() {
    running_.store(false);
    if (thread_.joinable()) {
        thread_.join();
    }
    // Final render
    if (is_tty()) {
        clear_lines(lines_printed_);
    }
    render();
    std::cout << "\n";
    std::cout.flush();
}

void Tui::clear_lines(int n) {
    for (int i = 0; i < n; ++i) {
        // Move cursor up one line, then clear line
        std::cout << "\x1b[A\x1b[2K";
    }
    if (n > 0) {
        std::cout << "\r";
        std::cout.flush();
    }
    lines_printed_ = 0;
}

std::string Tui::build_progress_bar(double pct, int width) const {
    if (width < 4) return "";
    int fill = (int)(pct / 100.0 * width);
    fill = utils::clamp(fill, 0, width);

    std::string bar = "[";
    for (int i = 0; i < width; ++i) {
        if (i < fill)          bar += '=';
        else if (i == fill)    bar += '>';
        else                   bar += ' ';
    }
    bar += "]";
    return bar;
}

void Tui::render() {
    auto now = std::chrono::steady_clock::now();
    double elapsed_s = std::chrono::duration<double>(now - last_time_).count();

    u64 bytes_sent  = state_.bytes_sent.load();
    u64 bytes_total = state_.bytes_total.load();
    u32 files_done  = state_.files_done.load();
    u32 files_total = state_.files_total.load();

    // Compute speed (EWMA)
    if (elapsed_s >= 0.05) {
        double instant_speed = (double)(bytes_sent - last_bytes_) / elapsed_s;
        if (last_bytes_ == 0) {
            smooth_speed_ = instant_speed;
        } else {
            smooth_speed_ = 0.7 * smooth_speed_ + 0.3 * instant_speed;
        }
        last_bytes_ = bytes_sent;
        last_time_  = now;
    }

    double pct = bytes_total > 0 ? (double)bytes_sent / bytes_total * 100.0 : 0.0;
    pct = utils::clamp(pct, 0.0, 100.0);

    // ETA
    std::string eta_str;
    if (smooth_speed_ > 0 && bytes_sent < bytes_total) {
        u64 remaining = bytes_total - bytes_sent;
        u64 eta_s = (u64)(remaining / smooth_speed_);
        eta_str = "ETA " + utils::format_duration_s(eta_s);
    } else {
        eta_str = "     ";
    }

    // Build display
    std::ostringstream ss;
    ss << std::fixed;

    // Line 1: Progress bar
    int bar_width = 40;
    std::string bar = build_progress_bar(pct, bar_width);
    ss << bar << " " << std::setw(5) << std::setprecision(1) << pct << "%";
    std::string line1 = ss.str();
    ss.str("");

    // Line 2: Stats
    ss << "  Files: " << files_done << "/" << files_total
       << "  " << state_.transfer_label << ": " << utils::format_bytes(bytes_sent)
       << "/" << utils::format_bytes(bytes_total)
       << "  Speed: " << utils::format_speed(smooth_speed_)
       << "  " << eta_str;
    std::string line2 = ss.str();
    ss.str("");

    // Line 3: Current file
    std::string current;
    {
        std::lock_guard<std::mutex> lk(state_.current_file_mutex);
        current = state_.current_file;
    }
    if (current.size() > 70) {
        current = "..." + current.substr(current.size() - 67);
    }
    std::string line3 = "  > " + current;

    if (!is_tty()) {
        // Non-TTY: just print a progress line periodically
        std::cout << line2 << "\n";
        std::cout.flush();
        return;
    }

    // Clear previous output
    clear_lines(lines_printed_);

    // Print new output
    std::cout << line1 << "\n" << line2 << "\n" << line3 << "\n";
    std::cout.flush();
    lines_printed_ = 3;
}
