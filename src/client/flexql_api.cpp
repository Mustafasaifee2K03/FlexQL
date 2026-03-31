#include "common/flexql.h"

#include <arpa/inet.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "network/protocol.h"
#include "network/tcp_client.h"

struct FlexQL {
    std::unique_ptr<flexql::network::TCPClient> client;
};

namespace {

void set_error(char** errmsg, const std::string& msg) {
    if (!errmsg) {
        return;
    }
    const std::size_t n = msg.size() + 1;
    auto* mem = static_cast<char*>(std::malloc(n));
    if (!mem) {
        *errmsg = nullptr;
        return;
    }
    std::memcpy(mem, msg.c_str(), n);
    *errmsg = mem;
}

}  // namespace

extern "C" {

int flexql_open(const char* host, int port, FlexQL** db) {
    if (!host || !db || port <= 0) {
        return FLEXQL_ERROR;
    }

    auto* handle = new FlexQL();
    handle->client = std::make_unique<flexql::network::TCPClient>();

    std::string err;
    if (!handle->client->connect_to(host, port, &err)) {
        delete handle;
        return FLEXQL_ERROR;
    }

    *db = handle;
    return FLEXQL_OK;
}

int flexql_close(FlexQL* db) {
    if (!db) {
        return FLEXQL_ERROR;
    }
    db->client->disconnect();
    delete db;
    return FLEXQL_OK;
}

int flexql_exec(FlexQL* db,
                const char* sql,
                int (*callback)(void*, int, char**, char**),
                void* arg,
                char** errmsg) {
    if (!db || !sql) {
        set_error(errmsg, "invalid handle or SQL");
        return FLEXQL_ERROR;
    }

    std::string net_err;
    if (!db->client->send_request(sql, &net_err)) {
        set_error(errmsg, net_err);
        return FLEXQL_ERROR;
    }

    std::vector<std::string> active_columns;
    std::vector<char*> active_column_ptrs;

    auto drain_until_result_end = [&]() -> bool {
        for (;;) {
            std::string payload;
            std::string recv_err;
            if (!db->client->recv_response_frame(&payload, &recv_err)) {
                return false;
            }

            flexql::network::ResponseFrame frame;
            std::string frame_err;
            if (!flexql::network::parse_response_frame(payload, &frame, &frame_err)) {
                return false;
            }

            if (frame.type == flexql::network::ResponseFrameType::RESULT_END ||
                frame.type == flexql::network::ResponseFrameType::OK) {
                return true;
            }
            if (frame.type == flexql::network::ResponseFrameType::ERR) {
                return false;
            }
        }
    };

    for (;;) {
        std::string payload;
        if (!db->client->recv_response_frame(&payload, &net_err)) {
            set_error(errmsg, net_err);
            return FLEXQL_ERROR;
        }

        flexql::network::ResponseFrame frame;
        std::string parse_err;
        if (!flexql::network::parse_response_frame(payload, &frame, &parse_err)) {
            const auto nl = payload.find('\n');
            const std::string head = nl == std::string::npos ? payload : payload.substr(0, nl);
            set_error(errmsg, parse_err + ": " + head);
            return FLEXQL_ERROR;
        }

        if (frame.type == flexql::network::ResponseFrameType::OK) {
            if (errmsg) {
                *errmsg = nullptr;
            }
            return FLEXQL_OK;
        }

        if (frame.type == flexql::network::ResponseFrameType::ERR) {
            set_error(errmsg, frame.message);
            return FLEXQL_ERROR;
        }

        if (frame.type == flexql::network::ResponseFrameType::RESULT_FULL) {
            flexql::network::ResponsePayload resp;
            if (!flexql::network::parse_response(payload, &resp, &parse_err)) {
                set_error(errmsg, parse_err);
                return FLEXQL_ERROR;
            }
            if (!resp.ok) {
                set_error(errmsg, resp.message);
                return FLEXQL_ERROR;
            }

            if (resp.has_result && callback) {
                std::vector<char*> col_names(resp.result.column_names.size(), nullptr);
                for (std::size_t i = 0; i < resp.result.column_names.size(); ++i) {
                    col_names[i] = const_cast<char*>(resp.result.column_names[i].c_str());
                }

                for (const auto& row : resp.result.rows) {
                    std::string flattened;
                    if (!row.empty()) {
                        flattened = row[0];
                        for (std::size_t i = 1; i < row.size(); ++i) {
                            flattened.push_back(' ');
                            flattened.append(row[i]);
                        }
                    }
                    char* value_ptr = const_cast<char*>(flattened.c_str());
                    if (callback(arg,
                                 1,
                                 &value_ptr,
                                 col_names.empty() ? nullptr : col_names.data()) == 1) {
                        break;
                    }
                }
            }
            if (errmsg) {
                *errmsg = nullptr;
            }
            return FLEXQL_OK;
        }

        if (frame.type == flexql::network::ResponseFrameType::RESULT_START) {
            active_columns = std::move(frame.column_names);
            active_column_ptrs.assign(active_columns.size(), nullptr);
            for (std::size_t i = 0; i < active_columns.size(); ++i) {
                active_column_ptrs[i] = const_cast<char*>(active_columns[i].c_str());
            }
            continue;
        }

        if (frame.type == flexql::network::ResponseFrameType::RESULT_ROWS) {
            if (!callback) {
                continue;
            }
            for (const auto& row : frame.rows) {
                std::string flattened;
                if (!row.empty()) {
                    flattened = row[0];
                    for (std::size_t i = 1; i < row.size(); ++i) {
                        flattened.push_back(' ');
                        flattened.append(row[i]);
                    }
                }
                char* value_ptr = const_cast<char*>(flattened.c_str());

                const int cb_rc = callback(arg,
                                           1,
                                           &value_ptr,
                                           active_column_ptrs.empty() ? nullptr : active_column_ptrs.data());
                if (cb_rc == 1) {
                    if (!drain_until_result_end()) {
                        set_error(errmsg, "failed to stop streamed query cleanly");
                        return FLEXQL_ERROR;
                    }
                    if (errmsg) {
                        *errmsg = nullptr;
                    }
                    return FLEXQL_OK;
                }
            }
            continue;
        }

        if (frame.type == flexql::network::ResponseFrameType::RESULT_END) {
            if (errmsg) {
                *errmsg = nullptr;
            }
            return FLEXQL_OK;
        }

        set_error(errmsg, "unsupported response frame");
        return FLEXQL_ERROR;
    }
}

int flexql_exec_bulk(FlexQL* db,
                    const char** sql_list,
                    int sql_count,
                    char** errmsg) {
    if (!db || !sql_list || sql_count <= 0) {
        set_error(errmsg, "invalid handle or SQL list");
        return FLEXQL_ERROR;
    }

    std::size_t total_bytes = 0;
    for (int i = 0; i < sql_count; ++i) {
        if (!sql_list[i]) {
            set_error(errmsg, "null SQL in SQL list");
            return FLEXQL_ERROR;
        }
        total_bytes += sizeof(std::uint32_t) + std::strlen(sql_list[i]);
    }

    std::string request_buf;
    request_buf.reserve(total_bytes);
    for (int i = 0; i < sql_count; ++i) {
        const std::size_t sql_len = std::strlen(sql_list[i]);
        const std::uint32_t net_len = htonl(static_cast<std::uint32_t>(sql_len));
        request_buf.append(reinterpret_cast<const char*>(&net_len), sizeof(net_len));
        request_buf.append(sql_list[i], sql_len);
    }

    std::string net_err;
    if (!db->client->send_raw(request_buf.data(), request_buf.size(), &net_err)) {
        set_error(errmsg, net_err);
        return FLEXQL_ERROR;
    }

    std::string inbound;
    inbound.reserve(static_cast<std::size_t>(sql_count) * 16);

    int completed = 0;
    while (completed < sql_count) {
        std::string chunk;
        if (!db->client->recv_some(&chunk, 4 * 1024 * 1024, &net_err)) {
            set_error(errmsg, net_err);
            return FLEXQL_ERROR;
        }
        inbound.append(chunk);

        std::size_t consumed = 0;
        while (inbound.size() - consumed >= sizeof(std::uint32_t)) {
            std::uint32_t net_len = 0;
            std::memcpy(&net_len, inbound.data() + consumed, sizeof(net_len));
            const std::uint32_t payload_len = ntohl(net_len);
            const std::size_t frame_len = sizeof(std::uint32_t) + static_cast<std::size_t>(payload_len);

            if (inbound.size() - consumed < frame_len) {
                break;
            }

            const char* payload = inbound.data() + consumed + sizeof(std::uint32_t);
            if (payload_len == 3 && std::memcmp(payload, "OK\n", 3) == 0) {
                ++completed;
                consumed += frame_len;
                continue;
            }

            std::string payload_str(payload, payload + payload_len);
            flexql::network::ResponseFrame frame;
            std::string parse_err;
            if (!flexql::network::parse_response_frame(payload_str, &frame, &parse_err)) {
                set_error(errmsg, "bulk response parse error: " + parse_err);
                return FLEXQL_ERROR;
            }

            if (frame.type == flexql::network::ResponseFrameType::ERR) {
                set_error(errmsg, frame.message.empty() ? "bulk request failed" : frame.message);
                return FLEXQL_ERROR;
            }
            if (frame.type != flexql::network::ResponseFrameType::OK) {
                set_error(errmsg, "bulk request received non-OK response frame");
                return FLEXQL_ERROR;
            }

            ++completed;
            consumed += frame_len;
        }

        if (consumed > 0) {
            inbound.erase(0, consumed);
        }
    }

    if (errmsg) {
        *errmsg = nullptr;
    }
    return FLEXQL_OK;
}

void flexql_free(void* ptr) {
    std::free(ptr);
}

}  // extern "C"
