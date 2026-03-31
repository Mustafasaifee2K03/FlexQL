#include "network/protocol.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>

#include "utils/string_utils.h"

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

bool recv_all(int fd, char* buf, std::size_t len) {
    std::size_t recvd = 0;
    while (recvd < len) {
        const auto n = ::recv(fd, buf + recvd, len - recvd, 0);
        if (n <= 0) {
            return false;
        }
        recvd += static_cast<std::size_t>(n);
    }
    return true;
}

std::string_view trim_view(std::string_view s) {
    while (!s.empty()) {
        const char c = s.front();
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
            s.remove_prefix(1);
            continue;
        }
        break;
    }
    while (!s.empty()) {
        const char c = s.back();
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
            s.remove_suffix(1);
            continue;
        }
        break;
    }
    return s;
}

}  // namespace

bool send_frame(int fd, const std::string& payload) {
    const std::uint32_t len = static_cast<std::uint32_t>(payload.size());
    const std::uint32_t net_len = htonl(len);
    if (!send_all(fd, reinterpret_cast<const char*>(&net_len), sizeof(net_len))) {
        return false;
    }
    if (len == 0) {
        return true;
    }
    return send_all(fd, payload.data(), payload.size());
}

bool send_frame_chunks(int fd,
                       const char* chunk_a,
                       std::size_t chunk_a_len,
                       const char* chunk_b,
                       std::size_t chunk_b_len) {
    if ((chunk_a_len > 0 && !chunk_a) || (chunk_b_len > 0 && !chunk_b)) {
        return false;
    }

    const auto total_len = chunk_a_len + chunk_b_len;
    const std::uint32_t len = static_cast<std::uint32_t>(total_len);
    const std::uint32_t net_len = htonl(len);
    if (!send_all(fd, reinterpret_cast<const char*>(&net_len), sizeof(net_len))) {
        return false;
    }
    if (chunk_a_len > 0 && !send_all(fd, chunk_a, chunk_a_len)) {
        return false;
    }
    if (chunk_b_len > 0 && !send_all(fd, chunk_b, chunk_b_len)) {
        return false;
    }
    return true;
}

bool recv_frame(int fd, std::string* payload) {
    if (!payload) {
        return false;
    }
    std::uint32_t net_len = 0;
    if (!recv_all(fd, reinterpret_cast<char*>(&net_len), sizeof(net_len))) {
        return false;
    }
    const std::uint32_t len = ntohl(net_len);
    payload->assign(len, '\0');
    if (len == 0) {
        return true;
    }
    return recv_all(fd, payload->data(), len);
}

bool recv_commands_bulk(int fd,
                        std::string* carry,
                        std::vector<std::string_view>* commands,
                        std::size_t* consumed_bytes) {
    if (!carry || !commands || !consumed_bytes) {
        return false;
    }

    commands->clear();
    *consumed_bytes = 0;

    constexpr std::size_t kBulkReadBytes = 16 * 1024 * 1024;
    static thread_local std::vector<char> read_buf(kBulkReadBytes);
    const auto n = ::recv(fd, read_buf.data(), read_buf.size(), 0);
    if (n <= 0) {
        return false;
    }

    carry->append(read_buf.data(), static_cast<std::size_t>(n));

    std::size_t consumed = 0;
    while (true) {
        // Primary path: length-prefixed frames (client protocol).
        if (carry->size() - consumed < sizeof(std::uint32_t)) {
            break;
        }

        std::uint32_t net_len = 0;
        std::memcpy(&net_len, carry->data() + consumed, sizeof(net_len));
        const std::uint32_t len = ntohl(net_len);

        constexpr std::uint32_t kMaxFrameBytes = 16U * 1024U * 1024U;
        if (len <= kMaxFrameBytes) {
            if (carry->size() - consumed < sizeof(std::uint32_t) + static_cast<std::size_t>(len)) {
                break;  // Wait for full frame; do not fall back to delimiter parsing.
            }
            const std::size_t payload_start = consumed + sizeof(std::uint32_t);
            if (len > 0) {
                commands->emplace_back(carry->data() + payload_start, static_cast<std::size_t>(len));
            }
            consumed = payload_start + static_cast<std::size_t>(len);
            continue;
        }

        // Fallback for plain-text clients that don't use framed protocol.
        std::size_t delim = consumed;
        while (delim < carry->size() && (*carry)[delim] != ';' && (*carry)[delim] != '\n') {
            ++delim;
        }
        if (delim >= carry->size()) {
            break;
        }

        const std::string_view raw(carry->data() + consumed, delim - consumed);
        const std::string_view command = trim_view(raw);
        if (!command.empty()) {
            commands->push_back(command);
        }

        consumed = delim + 1;
        while (consumed < carry->size() && ((*carry)[consumed] == ';' || (*carry)[consumed] == '\n' ||
                                            (*carry)[consumed] == '\r')) {
            ++consumed;
        }
    }

    *consumed_bytes = consumed;
    return true;
}

std::string serialize_ok_response() {
    return "OK\n";
}

std::string serialize_error_response(const std::string& message) {
    return "ERR\n" + message + "\n";
}

std::string serialize_result_start(const std::vector<std::string>& column_names) {
    std::string out;
    out += "RESULT_START\n";
    out += std::to_string(column_names.size()) + "\n";

    std::vector<std::string> escaped_cols;
    escaped_cols.reserve(column_names.size());
    for (const auto& c : column_names) {
        escaped_cols.push_back(utils::escape_field(c));
    }
    out += utils::join(escaped_cols, '\t') + "\n";
    return out;
}

std::string serialize_result_rows(const std::vector<std::vector<std::string>>& rows) {
    std::string out;
    out += "RESULT_ROWS\n";
    out += std::to_string(rows.size()) + "\n";

    for (const auto& row : rows) {
        std::vector<std::string> escaped_row;
        escaped_row.reserve(row.size());
        for (const auto& v : row) {
            escaped_row.push_back(utils::escape_field(v));
        }
        out += utils::join(escaped_row, '\t') + "\n";
    }
    return out;
}

std::string serialize_result_end() {
    return "RESULT_END\n";
}

bool parse_response_frame(const std::string& payload, ResponseFrame* out, std::string* err) {
    if (!out) {
        if (err) {
            *err = "output is null";
        }
        return false;
    }

    auto lines = utils::split(payload, '\n');
    if (lines.empty()) {
        if (err) {
            *err = "empty response";
        }
        return false;
    }

    out->type = ResponseFrameType::UNKNOWN;
    out->message.clear();
    out->column_names.clear();
    out->rows.clear();

    if (lines[0] == "OK") {
        out->type = ResponseFrameType::OK;
        out->message = "OK";
        return true;
    }

    if (lines[0] == "ERR") {
        out->type = ResponseFrameType::ERR;
        out->message = lines.size() > 1 ? lines[1] : "unknown error";
        return true;
    }

    if (lines[0] == "RESULT_END") {
        out->type = ResponseFrameType::RESULT_END;
        return true;
    }

    if (lines[0] == "RESULT_START") {
        if (lines.size() < 3) {
            if (err) {
                *err = "malformed RESULT_START frame";
            }
            return false;
        }
        int col_count = 0;
        try {
            col_count = std::stoi(lines[1]);
        } catch (...) {
            if (err) {
                *err = "invalid RESULT_START column count";
            }
            return false;
        }
        auto cols = utils::split_escaped_tab(lines[2]);
        if (static_cast<int>(cols.size()) != col_count) {
            if (err) {
                *err = "RESULT_START column count mismatch";
            }
            return false;
        }
        out->type = ResponseFrameType::RESULT_START;
        out->column_names = std::move(cols);
        return true;
    }

    if (lines[0] == "RESULT_ROWS") {
        if (lines.size() < 2) {
            if (err) {
                *err = "malformed RESULT_ROWS frame";
            }
            return false;
        }

        int row_count = 0;
        try {
            row_count = std::stoi(lines[1]);
        } catch (...) {
            if (err) {
                *err = "invalid RESULT_ROWS row count";
            }
            return false;
        }

        if (row_count < 0 || lines.size() < static_cast<std::size_t>(row_count + 2)) {
            if (err) {
                *err = "truncated RESULT_ROWS frame";
            }
            return false;
        }

        out->type = ResponseFrameType::RESULT_ROWS;
        out->rows.reserve(static_cast<std::size_t>(row_count));
        for (int i = 0; i < row_count; ++i) {
            out->rows.push_back(utils::split_escaped_tab(lines[static_cast<std::size_t>(i + 2)]));
        }
        return true;
    }

    if (lines[0] == "RESULT") {
        out->type = ResponseFrameType::RESULT_FULL;
        return true;
    }

    if (err) {
        *err = "unknown response type";
    }
    return false;
}

std::string serialize_response(const ExecResult& res) {
    if (!res.ok) {
        return serialize_error_response(res.message);
    }
    if (res.result_set.column_names.empty()) {
        return serialize_ok_response();
    }

    std::string out;
    out += "RESULT\n";
    out += std::to_string(res.result_set.column_names.size()) + "\n";

    std::vector<std::string> escaped_cols;
    escaped_cols.reserve(res.result_set.column_names.size());
    for (const auto& c : res.result_set.column_names) {
        escaped_cols.push_back(utils::escape_field(c));
    }
    out += utils::join(escaped_cols, '\t') + "\n";
    out += std::to_string(res.result_set.rows.size()) + "\n";

    for (const auto& row : res.result_set.rows) {
        std::vector<std::string> escaped_row;
        escaped_row.reserve(row.size());
        for (const auto& v : row) {
            escaped_row.push_back(utils::escape_field(v));
        }
        out += utils::join(escaped_row, '\t') + "\n";
    }
    out += "END\n";
    return out;
}

bool parse_response(const std::string& payload, ResponsePayload* out, std::string* err) {
    if (!out) {
        if (err) {
            *err = "output is null";
        }
        return false;
    }

    auto lines = utils::split(payload, '\n');
    if (lines.empty()) {
        if (err) {
            *err = "empty response";
        }
        return false;
    }

    if (lines[0] == "OK") {
        out->ok = true;
        out->message = "OK";
        out->has_result = false;
        return true;
    }

    if (lines[0] == "ERR") {
        out->ok = false;
        out->message = lines.size() > 1 ? lines[1] : "unknown error";
        out->has_result = false;
        return true;
    }

    if (lines[0] != "RESULT") {
        if (err) {
            *err = "unknown response type";
        }
        return false;
    }

    if (lines.size() < 5) {
        if (err) {
            *err = "malformed result response";
        }
        return false;
    }

    std::size_t idx = 1;
    const int col_count = std::stoi(lines[idx++]);
    auto cols = utils::split_escaped_tab(lines[idx++]);
    if (static_cast<int>(cols.size()) != col_count) {
        if (err) {
            *err = "column count mismatch in response";
        }
        return false;
    }

    const int row_count = std::stoi(lines[idx++]);
    ResultSet rs;
    rs.column_names = std::move(cols);

    for (int i = 0; i < row_count; ++i) {
        if (idx >= lines.size()) {
            if (err) {
                *err = "truncated rows";
            }
            return false;
        }
        if (lines[idx] == "END") {
            if (err) {
                *err = "row count larger than payload";
            }
            return false;
        }
        auto row = utils::split_escaped_tab(lines[idx++]);
        rs.rows.push_back(std::move(row));
    }

    if (idx >= lines.size() || lines[idx] != "END") {
        if (err) {
            *err = "missing END marker";
        }
        return false;
    }

    out->ok = true;
    out->message = "OK";
    out->has_result = true;
    out->result = std::move(rs);
    return true;
}

}  // namespace flexql::network
