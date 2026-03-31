#include <iostream>
#include <string>

#include "common/flexql.h"

namespace {

int print_callback(void*, int col_count, char** values, char** col_names) {
    for (int i = 0; i < col_count; ++i) {
        std::cout << col_names[i] << "=" << values[i];
        if (i + 1 < col_count) {
            std::cout << " | ";
        }
    }
    std::cout << "\n";
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    const char* host = "127.0.0.1";
    int port = 9000;

    if (argc > 1) {
        host = argv[1];
    }
    if (argc > 2) {
        port = std::stoi(argv[2]);
    }

    FlexQL* db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "Failed to connect to FlexQL server\n";
        return 1;
    }

    std::cout << "Connected to FlexQL at " << host << ":" << port << "\n";
    std::cout << "Enter SQL (type quit or exit to leave)\n";

    std::string line;
    while (true) {
        std::cout << "flexql> ";
        if (!std::getline(std::cin, line)) {
            break;
        }

        if (line == "quit" || line == "exit") {
            break;
        }
        if (line.empty()) {
            continue;
        }

        char* errmsg = nullptr;
        const int rc = flexql_exec(db, line.c_str(), print_callback, nullptr, &errmsg);
        if (rc != FLEXQL_OK) {
            std::cerr << "Error: " << (errmsg ? errmsg : "unknown") << "\n";
            if (errmsg) {
                flexql_free(errmsg);
            }
        } else {
            std::cout << "OK\n";
        }
    }

    (void)flexql_close(db);
    return 0;
}
