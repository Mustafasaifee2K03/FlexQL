#pragma once

#include <cstddef>
#include <string>

namespace flexql::network {

class TCPClient {
public:
    TCPClient() = default;
    ~TCPClient();

    bool connect_to(const std::string& host, int port, std::string* err);
    void disconnect();

    bool send_request(const std::string& request, std::string* err);
    bool recv_response_frame(std::string* response_frame, std::string* err);
    bool send_raw(const char* data, std::size_t len, std::string* err);
    bool recv_some(std::string* out, std::size_t max_bytes, std::string* err);

    bool execute(const std::string& request, std::string* response, std::string* err);

private:
    int sock_fd_{-1};
};

}  // namespace flexql::network
