#pragma once

// ============================================================
// logger.hpp -- Thread-safe logger
// ============================================================

#include "platform.hpp"
#include <string>
#include <mutex>
#include <fstream>
#include <iostream>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <memory>

enum class LogLevel {
    DEBUG = 0,
    INFO  = 1,
    WARN  = 2,
    ERR   = 3,
};

class Logger {
public:
    static Logger& get() {
        static Logger instance;
        return instance;
    }

    void set_level(LogLevel lvl) {
        std::lock_guard<std::mutex> lk(mutex_);
        level_ = lvl;
    }

    void set_log_file(const std::string& path) {
        std::lock_guard<std::mutex> lk(mutex_);
        file_.open(path, std::ios::app);
    }

    void log(LogLevel lvl, const std::string& msg) {
        if (lvl < level_) return;
        std::string line = format_line(lvl, msg);
        std::lock_guard<std::mutex> lk(mutex_);
        if (lvl >= LogLevel::WARN) {
            std::cerr << line << "\n";
        } else {
            std::cout << line << "\n";
        }
        if (file_.is_open()) {
            file_ << line << "\n";
            file_.flush();
        }
    }

    void info(const std::string& msg)  { log(LogLevel::INFO, msg); }
    void warn(const std::string& msg)  { log(LogLevel::WARN, msg); }
    void error(const std::string& msg) { log(LogLevel::ERR,  msg); }
    void debug(const std::string& msg) { log(LogLevel::DEBUG, msg); }

    // Log transfer error to a dedicated error log
    void transfer_error(const std::string& msg) {
        std::string line = format_line(LogLevel::ERR, "[TRANSFER] " + msg);
        std::lock_guard<std::mutex> lk(mutex_);
        std::cerr << line << "\n";
        if (!transfer_err_file_.is_open()) {
            transfer_err_file_.open("transfer_errors.log", std::ios::app);
        }
        if (transfer_err_file_.is_open()) {
            transfer_err_file_ << line << "\n";
            transfer_err_file_.flush();
        }
    }

    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

private:
    Logger() : level_(LogLevel::INFO) {}

    std::string format_line(LogLevel lvl, const std::string& msg) {
        auto now = std::chrono::system_clock::now();
        auto t   = std::chrono::system_clock::to_time_t(now);
        auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(
                       now.time_since_epoch()) % 1000;

        std::ostringstream ss;
        ss << std::put_time(std::localtime(&t), "%Y-%m-%d %H:%M:%S");
        ss << '.' << std::setfill('0') << std::setw(3) << ms.count();
        ss << " [" << level_str(lvl) << "] " << msg;
        return ss.str();
    }

    static const char* level_str(LogLevel lvl) {
        switch (lvl) {
            case LogLevel::DEBUG: return "DEBUG";
            case LogLevel::INFO:  return "INFO ";
            case LogLevel::WARN:  return "WARN ";
            case LogLevel::ERR:   return "ERROR";
        }
        return "?????";
    }

    std::mutex    mutex_;
    LogLevel      level_;
    std::ofstream file_;
    std::ofstream transfer_err_file_;
};

// Convenience macros
#define LOG_INFO(msg)  Logger::get().info(msg)
#define LOG_WARN(msg)  Logger::get().warn(msg)
#define LOG_ERROR(msg) Logger::get().error(msg)
#define LOG_DEBUG(msg) Logger::get().debug(msg)
