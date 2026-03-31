#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include "concurrency/thread_pool.h"

namespace flexql::network {

class TCPServer {
public:
    using Handler = std::function<bool(std::string_view, int, std::string*, std::string*)>;

    TCPServer();
    ~TCPServer();

    bool start(const std::string& host,
               int port,
               std::size_t worker_threads,
               Handler handler,
               std::string* err);

    void stop();

private:
    void accept_loop();
    void handle_client(int client_fd);

    int listen_fd_{-1};
    std::atomic<bool> running_{false};
    std::thread accept_thread_;
    std::unique_ptr<ThreadPool> pool_;
    Handler handler_;
};

}  // namespace flexql::network
