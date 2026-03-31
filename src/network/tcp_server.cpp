#include "network/tcp_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <vector>

#include "network/protocol.h"

namespace flexql::network {

namespace {

bool send_all(int fd, const char* buf, std::size_t len) {
    std::size_t sent = 0;
    while (sent < len) {
        const auto n = ::send(fd, buf + sent, len - sent, 0);
        if (n <= 0) {
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

}  // namespace

TCPServer::TCPServer() = default;

TCPServer::~TCPServer() {
    stop();
}

bool TCPServer::start(const std::string& host,
                      int port,
                      std::size_t worker_threads,
                      Handler handler,
                      std::string* err) {
    if (running_) {
        if (err) {
            *err = "server already running";
        }
        return false;
    }

    handler_ = std::move(handler);
    pool_ = std::make_unique<ThreadPool>(worker_threads == 0 ? 1 : worker_threads);

    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        if (err) {
            *err = "socket creation failed";
        }
        return false;
    }

    int one = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (host == "0.0.0.0" || host == "localhost") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
            if (err) {
                *err = "invalid host address";
            }
            ::close(listen_fd_);
            listen_fd_ = -1;
            return false;
        }
    }

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        if (err) {
            *err = std::string("bind failed: ") + std::strerror(errno);
        }
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    if (::listen(listen_fd_, 128) != 0) {
        if (err) {
            *err = "listen failed";
        }
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    running_ = true;
    accept_thread_ = std::thread([this] { accept_loop(); });
    return true;
}

void TCPServer::stop() {
    if (!running_) {
        return;
    }

    running_ = false;
    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    if (pool_) {
        pool_->stop();
        pool_.reset();
    }
}

void TCPServer::accept_loop() {
    while (running_) {
        const int client_fd = ::accept(listen_fd_, nullptr, nullptr);
        if (client_fd < 0) {
            if (running_) {
                continue;
            }
            break;
        }

        int one = 1;
        ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        if (pool_) {
            pool_->enqueue([this, client_fd] { handle_client(client_fd); });
        } else {
            ::shutdown(client_fd, SHUT_RDWR);
            ::close(client_fd);
        }
    }
}

void TCPServer::handle_client(int client_fd) {
    std::string carry;
    std::vector<std::string_view> commands;
    std::string send_buffer;
    send_buffer.reserve(16 * 1024 * 1024);

    while (running_) {
        std::size_t consumed_bytes = 0;
        if (!recv_commands_bulk(client_fd, &carry, &commands, &consumed_bytes)) {
            break;
        }

        for (const auto& req : commands) {
            std::string handler_err;
            bool ok = false;
            if (handler_) {
                ok = handler_(req, client_fd, &send_buffer, &handler_err);
            } else {
                handler_err = "internal handler not set";
            }

            if (!ok) {
                if (!handler_err.empty()) {
                    if (!send_buffer.empty()) {
                        if (!send_all(client_fd, send_buffer.data(), send_buffer.size())) {
                            ::shutdown(client_fd, SHUT_RDWR);
                            ::close(client_fd);
                            return;
                        }
                        send_buffer.clear();
                    }
                    const std::string err_payload = serialize_error_response(handler_err);
                    if (!send_frame(client_fd, err_payload)) {
                        ::shutdown(client_fd, SHUT_RDWR);
                        ::close(client_fd);
                        return;
                    }
                    continue;
                }
                ::shutdown(client_fd, SHUT_RDWR);
                ::close(client_fd);
                return;
            }
        }

        if (!send_buffer.empty()) {
            if (!send_all(client_fd, send_buffer.data(), send_buffer.size())) {
                break;
            }
            send_buffer.clear();
        }

        if (consumed_bytes > 0) {
            carry.erase(0, consumed_bytes);
        }

        if (commands.empty()) {
            continue;
        }
    }

    ::shutdown(client_fd, SHUT_RDWR);
    ::close(client_fd);
}

}  // namespace flexql::network
