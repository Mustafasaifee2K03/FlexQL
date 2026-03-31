#include "network/tcp_client.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

#include "network/protocol.h"

namespace flexql::network {

TCPClient::~TCPClient() {
    disconnect();
}

bool TCPClient::connect_to(const std::string& host, int port, std::string* err) {
    disconnect();

    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* res = nullptr;
    const int rc = getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res);
    if (rc != 0) {
        if (err) {
            *err = gai_strerror(rc);
        }
        return false;
    }

    for (auto p = res; p != nullptr; p = p->ai_next) {
        const int fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) {
            continue;
        }
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
            sock_fd_ = fd;
            freeaddrinfo(res);
            return true;
        }
        ::close(fd);
    }

    freeaddrinfo(res);
    if (err) {
        *err = "failed to connect";
    }
    return false;
}

void TCPClient::disconnect() {
    if (sock_fd_ >= 0) {
        ::close(sock_fd_);
        sock_fd_ = -1;
    }
}

bool TCPClient::send_request(const std::string& request, std::string* err) {
    if (sock_fd_ < 0) {
        if (err) {
            *err = "not connected";
        }
        return false;
    }

    if (!send_frame(sock_fd_, request)) {
        if (err) {
            *err = "failed to send request";
        }
        return false;
    }
    return true;
}

bool TCPClient::recv_response_frame(std::string* response_frame, std::string* err) {
    if (sock_fd_ < 0) {
        if (err) {
            *err = "not connected";
        }
        return false;
    }
    if (!recv_frame(sock_fd_, response_frame)) {
        if (err) {
            *err = "failed to receive response";
        }
        return false;
    }
    return true;
}

bool TCPClient::send_raw(const char* data, std::size_t len, std::string* err) {
    if (sock_fd_ < 0) {
        if (err) {
            *err = "not connected";
        }
        return false;
    }
    if (len == 0) {
        return true;
    }

    std::size_t sent = 0;
    while (sent < len) {
        const auto n = ::send(sock_fd_, data + sent, len - sent, 0);
        if (n <= 0) {
            if (err) {
                *err = "failed to send request";
            }
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

bool TCPClient::recv_some(std::string* out, std::size_t max_bytes, std::string* err) {
    if (!out) {
        if (err) {
            *err = "output buffer is null";
        }
        return false;
    }
    if (sock_fd_ < 0) {
        if (err) {
            *err = "not connected";
        }
        return false;
    }
    if (max_bytes == 0) {
        out->clear();
        return true;
    }

    out->assign(max_bytes, '\0');
    const auto n = ::recv(sock_fd_, out->data(), max_bytes, 0);
    if (n <= 0) {
        if (err) {
            *err = "failed to receive response";
        }
        out->clear();
        return false;
    }
    out->resize(static_cast<std::size_t>(n));
    return true;
}

bool TCPClient::execute(const std::string& request, std::string* response, std::string* err) {
    if (!send_request(request, err)) {
        return false;
    }
    return recv_response_frame(response, err);
}

}  // namespace flexql::network
