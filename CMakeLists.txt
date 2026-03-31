cmake_minimum_required(VERSION 3.16)
project(flexql LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -march=native -mtune=native -flto -pthread")

include_directories(include)

set(FLEXQL_CORE_SOURCES
    src/utils/string_utils.cpp
    src/utils/time_utils.cpp
    src/storage/storage_engine.cpp
    src/index/primary_index.cpp
    src/cache/lru_cache.cpp
    src/parser/sql_parser.cpp
    src/query/query_executor.cpp
    src/network/protocol.cpp
    src/network/tcp_client.cpp
    src/network/tcp_server.cpp
    src/expiration/expiration.cpp
    src/concurrency/thread_pool.cpp
)

add_library(flexql_client_api STATIC
    src/client/flexql_api.cpp
    src/network/tcp_client.cpp
    src/network/protocol.cpp
    src/utils/string_utils.cpp
)
target_include_directories(flexql_client_api PUBLIC include)

add_executable(flexql_server
    src/server/main.cpp
    ${FLEXQL_CORE_SOURCES}
)
target_include_directories(flexql_server PUBLIC include)

add_executable(flexql_client
    src/client/repl.cpp
)
target_link_libraries(flexql_client PRIVATE flexql_client_api)
target_include_directories(flexql_client PUBLIC include)

add_executable(benchmark_flexql
    scripts/benchmark_flexql.cpp
)
target_link_libraries(benchmark_flexql PRIVATE flexql_client_api)
target_include_directories(benchmark_flexql PUBLIC include)

add_executable(benchmark_flexql2
    scripts/benchmark_flexql2.cpp
)
target_link_libraries(benchmark_flexql2 PRIVATE flexql_client_api)
target_include_directories(benchmark_flexql2 PUBLIC include)

add_executable(benchmark_flexql_extreme
    scripts/benchmark_flexql_extreme.cpp
)
target_link_libraries(benchmark_flexql_extreme PRIVATE flexql_client_api)
target_include_directories(benchmark_flexql_extreme PUBLIC include)

add_executable(benchmark_flexql_isolated
    scripts/benchmark_flexql_isolated.cpp
)
target_link_libraries(benchmark_flexql_isolated PRIVATE flexql_client_api)
target_include_directories(benchmark_flexql_isolated PUBLIC include)
