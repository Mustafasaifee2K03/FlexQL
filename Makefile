CXX := g++
CXXFLAGS := -std=c++17 -O3 -march=native -mtune=native -flto=auto -pthread -Iinclude
# jemalloc is optional. Enable it with: make USE_JEMALLOC=1
JEMALLOC_LIBS :=
ifeq ($(USE_JEMALLOC),1)
JEMALLOC_LIBS := -ljemalloc
endif

SERVER_SOURCES := \
	src/server/main.cpp \
	src/utils/string_utils.cpp \
	src/utils/time_utils.cpp \
	src/storage/storage_engine.cpp \
	src/index/primary_index.cpp \
	src/cache/lru_cache.cpp \
	src/parser/sql_parser.cpp \
	src/query/query_executor.cpp \
	src/network/protocol.cpp \
	src/network/tcp_server.cpp \
	src/expiration/expiration.cpp \
	src/concurrency/thread_pool.cpp

CLIENT_SOURCES := \
	src/client/repl.cpp \
	src/client/flexql_api.cpp \
	src/network/protocol.cpp \
	src/network/tcp_client.cpp \
	src/utils/string_utils.cpp

BENCHMARK_FLEXQL_SOURCES := \
	scripts/benchmark_flexql.cpp \
	src/client/flexql_api.cpp \
	src/network/protocol.cpp \
	src/network/tcp_client.cpp \
	src/utils/string_utils.cpp

BENCHMARK_FLEXQL2_SOURCES := \
	scripts/benchmark_flexql2.cpp \
	src/client/flexql_api.cpp \
	src/network/protocol.cpp \
	src/network/tcp_client.cpp \
	src/utils/string_utils.cpp

BENCHMARK_FLEXQL_EXTREME_SOURCES := \
	scripts/benchmark_flexql_extreme.cpp \
	src/client/flexql_api.cpp \
	src/network/protocol.cpp \
	src/network/tcp_client.cpp \
	src/utils/string_utils.cpp

BENCHMARK_FLEXQL_ISOLATED_SOURCES := \
	scripts/benchmark_flexql_isolated.cpp \
	src/client/flexql_api.cpp \
	src/network/protocol.cpp \
	src/network/tcp_client.cpp \
	src/utils/string_utils.cpp

all: bin/flexql_server bin/flexql_client bin/benchmark_flexql bin/benchmark_flexql2 bin/benchmark_flexql_extreme bin/benchmark_flexql_isolated

bin:
	mkdir -p bin

bin/flexql_server: $(SERVER_SOURCES) | bin
	$(CXX) $(CXXFLAGS) $(SERVER_SOURCES) -o $@ $(JEMALLOC_LIBS)

bin/flexql_client: $(CLIENT_SOURCES) | bin
	$(CXX) $(CXXFLAGS) $(CLIENT_SOURCES) -o $@

bin/benchmark_flexql: $(BENCHMARK_FLEXQL_SOURCES) | bin
	$(CXX) $(CXXFLAGS) $(BENCHMARK_FLEXQL_SOURCES) -o $@ $(JEMALLOC_LIBS)

bin/benchmark_flexql2: $(BENCHMARK_FLEXQL2_SOURCES) | bin
	$(CXX) $(CXXFLAGS) $(BENCHMARK_FLEXQL2_SOURCES) -o $@

bin/benchmark_flexql_extreme: $(BENCHMARK_FLEXQL_EXTREME_SOURCES) | bin
	$(CXX) $(CXXFLAGS) $(BENCHMARK_FLEXQL_EXTREME_SOURCES) -o $@

bin/benchmark_flexql_isolated: $(BENCHMARK_FLEXQL_ISOLATED_SOURCES) | bin
	$(CXX) $(CXXFLAGS) $(BENCHMARK_FLEXQL_ISOLATED_SOURCES) -o $@

clean:
	rm -f bin/flexql_server bin/flexql_client bin/benchmark_flexql bin/benchmark_flexql2 bin/benchmark_flexql_extreme bin/benchmark_flexql_isolated

.PHONY: all clean
