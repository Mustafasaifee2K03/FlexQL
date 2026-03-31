#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <sys/resource.h>

#include "cache/lru_cache.h"
#include "network/protocol.h"
#include "network/tcp_server.h"
#include "query/query_executor.h"
#include "storage/storage_engine.h"

namespace {
std::atomic<bool> g_stop{false};

void on_signal(int) {
    g_stop = true;
}
}  // namespace

int main(int argc, char** argv) {

    struct rlimit rl;
    rl.rlim_cur = 65535;
    rl.rlim_max = 65535;
    setrlimit(RLIMIT_NOFILE, &rl);
    {
        // Raise file descriptor limit so wide-column tables can map many files safely.
        struct rlimit lim {};
        if (::getrlimit(RLIMIT_NOFILE, &lim) == 0) {
            const rlim_t target = static_cast<rlim_t>(65535);
            if (lim.rlim_cur < target) {
                lim.rlim_cur = target;
                if (lim.rlim_max < lim.rlim_cur) {
                    lim.rlim_max = lim.rlim_cur;
                }
                if (::setrlimit(RLIMIT_NOFILE, &lim) != 0) {
                    std::cerr << "Warning: failed to raise RLIMIT_NOFILE: "
                              << std::strerror(errno) << "\n";
                }
            }
        } else {
            std::cerr << "Warning: failed to read RLIMIT_NOFILE: "
                      << std::strerror(errno) << "\n";
        }
    }

    std::string host = "0.0.0.0";
    int port = 9000;
    std::size_t workers = std::max<std::size_t>(1, std::thread::hardware_concurrency());
    std::string data_dir = "data";

    if (argc > 1) {
        port = std::stoi(argv[1]);
    }
    if (argc > 2) {
        workers = static_cast<std::size_t>(std::stoul(argv[2]));
    }
    if (argc > 3) {
        data_dir = argv[3];
    }

    if (const char* env_workers = std::getenv("FLEXQL_WORKERS"); env_workers && *env_workers != '\0') {
        try {
            const auto v = static_cast<std::size_t>(std::stoull(env_workers));
            if (v > 0) {
                workers = v;
            }
        } catch (...) {
        }
    }

    std::size_t cache_capacity = 1024;
    if (const char* env_cache = std::getenv("FLEXQL_QUERY_CACHE_CAPACITY"); env_cache && *env_cache != '\0') {
        try {
            const auto v = static_cast<std::size_t>(std::stoull(env_cache));
            if (v > 0) {
                cache_capacity = v;
            }
        } catch (...) {
        }
    }

    flexql::StorageEngine storage(data_dir);
    std::string init_err;
    if (!storage.initialize(&init_err)) {
        std::cerr << "Storage initialization failed: " << init_err << "\n";
        return 1;
    }

    flexql::LRUCache cache(cache_capacity);
    flexql::QueryExecutor executor(&storage, &cache);

    flexql::network::TCPServer server;
    std::string err;
    if (!server.start(host, port, workers,
                      [&](std::string_view sql,
                          int client_fd,
                          std::string* send_buffer,
                          std::string* handler_err) {
                          return executor.stream_sql_to_fd(sql, client_fd, send_buffer, handler_err);
                      },
                      &err)) {
        std::cerr << "Server start failed: " << err << "\n";
        return 1;
    }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    std::cout << "FlexQL server running on " << host << ":" << port
              << " with " << workers << " worker threads" << "\n";

    while (!g_stop) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    server.stop();
    std::cout << "FlexQL server stopped\n";
    return 0;
}
