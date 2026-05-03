CXX := c++
CXXFLAGS := -std=c++20 -O3 -I.
BUILD_DIR := build

SRCS := test_spsc.cpp benchmark.cpp

.PHONY: all clean

all: $(BUILD_DIR)/tests $(BUILD_DIR)/bench

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/tests: $(BUILD_DIR) test_spsc.cpp spsc_queue.hpp market_feed.hpp
	$(CXX) $(CXXFLAGS) test_spsc.cpp -o $(BUILD_DIR)/tests -pthread

$(BUILD_DIR)/bench: $(BUILD_DIR) benchmark.cpp spsc_queue.hpp market_feed.hpp
	$(CXX) $(CXXFLAGS) benchmark.cpp -o $(BUILD_DIR)/bench -pthread

clean:
	rm -rf $(BUILD_DIR)
