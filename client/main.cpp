// ============================================================
// client/main.cpp -- FastCP client entry point
// ============================================================

#include "../common/platform.hpp"
#include "../common/logger.hpp"
#include "../common/utils.hpp"
#include "client_app.hpp"
#include <iostream>
#include <string>
#include <cstdlib>
#include <cstring>
#include <csignal>

static ClientApp* g_app = nullptr;

static void sig_handler(int /*sig*/) {
    if (g_app) g_app->stop();
}

static void print_usage(const char* prog) {
    std::cerr
        << "Usage: " << prog << " <dst_dir> <ip> <port> [options]\n"
        << "\n"
        << "  dst_dir         destination directory for received files\n"
        << "  ip              fastcp_server IP address\n"
        << "  port            TCP port (e.g. 9999)\n"
        << "\nOptions:\n"
        << "  --conns N       number of parallel connections (default: 4)\n"
        << "  --retry N       seconds to retry connecting if server not ready (default: 30)\n"
        << "  --verbose       enable debug logging\n"
        << "\nThe client can be started before the server; it will retry with\n"
        << "exponential back-off until the server becomes available.\n"
        << "\nExamples:\n"
        << "  " << prog << " /home/user/data 192.168.1.1 9999\n"
        << "  " << prog << " /home/user/data 192.168.1.1 9999 --conns 8\n"
        << "  " << prog << " /home/user/data 192.168.1.1 9999 --retry 60\n";
}

int main(int argc, char* argv[]) {
    platform::Guard platform_guard;

#ifndef _WIN32
    // Prevent SIGPIPE from terminating the process when writing to a broken
    // socket.  We rely on errno/EPIPE instead.
    signal(SIGPIPE, SIG_IGN);
#endif

    if (argc < 4) {
        print_usage(argv[0]);
        return 1;
    }

    std::string dst_dir  = argv[1];
    std::string ip       = argv[2];
    int port_int         = std::atoi(argv[3]);
    int retry_secs       = 30;
    int num_conns        = 4;

    for (int i = 4; i < argc; ++i) {
        if (std::strcmp(argv[i], "--retry") == 0 && i + 1 < argc) {
            retry_secs = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--conns") == 0 && i + 1 < argc) {
            num_conns = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--verbose") == 0) {
            Logger::get().set_level(LogLevel::DEBUG);
        } else {
            std::cerr << "Unknown option: " << argv[i] << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    if (!utils::validate_path(dst_dir)) {
        std::cerr << "ERROR: Invalid dst_dir\n";
        return 1;
    }
    if (!utils::validate_ip(ip)) {
        std::cerr << "ERROR: Invalid IP address: " << ip << "\n";
        return 1;
    }
    if (!utils::validate_port(port_int)) {
        std::cerr << "ERROR: Invalid port: " << port_int << "\n";
        return 1;
    }

    Logger::get().set_level(LogLevel::INFO);

    try {
        ClientApp app(dst_dir, ip, (u16)port_int, retry_secs, num_conns);
        g_app = &app;

        std::signal(SIGINT,  sig_handler);
        std::signal(SIGTERM, sig_handler);

        return app.run();
    } catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 2;
    }
}
