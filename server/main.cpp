// ============================================================
// server/main.cpp -- FastCP server entry point
// ============================================================

#include "../common/platform.hpp"
#include "../common/logger.hpp"
#include "../common/utils.hpp"
#include "server_app.hpp"
#include <iostream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <csignal>

static ServerApp* g_app = nullptr;

static void sig_handler(int /*sig*/) {
    if (g_app) g_app->stop();
}

static void print_usage(const char* prog) {
    std::cerr
        << "Usage: " << prog << " <src_dir> <ip> <port> [options]\n"
        << "\n"
        << "  src_dir        source directory to serve\n"
        << "  ip             IP address to listen on (use 0.0.0.0 for all interfaces)\n"
        << "  port           TCP port (e.g. 9999)\n"
        << "\nOptions:\n"
        << "  --conns N      expected parallel connections per client (default: 4)\n"
        << "  --no-compress  disable compression\n"
        << "  --chunk-kb N   chunk size in KB (default: 1024)\n"
        << "  --verbose      enable debug logging\n"
        << "\nThe server starts first, listens indefinitely, and accepts multiple\n"
        << "sequential client transfer sessions.\n"
        << "\nExample:\n"
        << "  " << prog << " /opt/share 0.0.0.0 9999\n";
}

int main(int argc, char* argv[]) {
    platform::Guard platform_guard;

    if (argc < 4) {
        print_usage(argv[0]);
        return 1;
    }

    ServerConfig cfg;
    cfg.src_dir    = argv[1];
    cfg.listen_ip  = argv[2];
    int port_int   = std::atoi(argv[3]);

    for (int i = 4; i < argc; ++i) {
        if (std::strcmp(argv[i], "--conns") == 0 && i + 1 < argc) {
            cfg.num_conns = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--no-compress") == 0) {
            cfg.use_compress = false;
        } else if (std::strcmp(argv[i], "--chunk-kb") == 0 && i + 1 < argc) {
            int kb = std::atoi(argv[++i]);
            cfg.chunk_size = (u32)kb * 1024;
        } else if (std::strcmp(argv[i], "--verbose") == 0) {
            Logger::get().set_level(LogLevel::DEBUG);
        } else {
            std::cerr << "Unknown option: " << argv[i] << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    if (!utils::validate_path(cfg.src_dir)) {
        std::cerr << "ERROR: Invalid src_dir\n";
        return 1;
    }
    if (cfg.listen_ip != "0.0.0.0" && !utils::validate_ip(cfg.listen_ip)) {
        std::cerr << "ERROR: Invalid IP address: " << cfg.listen_ip << "\n";
        return 1;
    }
    if (!utils::validate_port(port_int)) {
        std::cerr << "ERROR: Invalid port: " << port_int << "\n";
        return 1;
    }
    if (cfg.num_conns < 1 || cfg.num_conns > 64) {
        std::cerr << "ERROR: --conns must be 1-64\n";
        return 1;
    }
    if (cfg.chunk_size < 4096 || cfg.chunk_size > 64 * 1024 * 1024) {
        std::cerr << "ERROR: --chunk-kb must be 4-65536\n";
        return 1;
    }

    cfg.listen_port = (u16)port_int;
    Logger::get().set_level(LogLevel::INFO);

    try {
        ServerApp app(std::move(cfg));
        g_app = &app;

        std::signal(SIGINT,  sig_handler);
        std::signal(SIGTERM, sig_handler);

        return app.run();
    } catch (const std::exception& e) {
        std::cerr << "FATAL: " << e.what() << "\n";
        return 2;
    }
}
