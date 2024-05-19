cmake_minimum_required(VERSION 3.28)
project(codecrafters_redis C)

set(CMAKE_C_STANDARD 17)

add_executable(codecrafters_redis app/server.c app/table.h app/table.c)
