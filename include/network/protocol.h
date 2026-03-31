#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "common/types.h"

namespace flexql::network {

enum class ResponseFrameType {
    UNKNOWN = 0,
    OK,
    ERR,
    RESULT_START,
    RESULT_ROWS,
    RESULT_END,
    RESULT_FULL
};

struct ResponseFrame {
    ResponseFrameType type{ResponseFrameType::UNKNOWN};
    std::string message;
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string>> rows;
};

struct ResponsePayload {
    bool ok{false};
    std::string message;
    bool has_result{false};
    ResultSet result;
};

bool send_frame(int fd, const std::string& payload);
bool send_frame_chunks(int fd,
                       const char* chunk_a,
                       std::size_t chunk_a_len,
                       const char* chunk_b,
                       std::size_t chunk_b_len);
bool recv_frame(int fd, std::string* payload);
bool recv_commands_bulk(int fd,
                        std::string* carry,
                        std::vector<std::string_view>* commands,
                        std::size_t* consumed_bytes);

std::string serialize_ok_response();
std::string serialize_error_response(const std::string& message);
std::string serialize_result_start(const std::vector<std::string>& column_names);
std::string serialize_result_rows(const std::vector<std::vector<std::string>>& rows);
std::string serialize_result_end();

bool parse_response_frame(const std::string& payload, ResponseFrame* out, std::string* err);

std::string serialize_response(const ExecResult& res);
bool parse_response(const std::string& payload, ResponsePayload* out, std::string* err);

}  // namespace flexql::network
