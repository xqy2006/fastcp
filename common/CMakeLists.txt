set(COMMON_SOURCES
    socket.cpp
    file_io.cpp
    tui.cpp
)

add_library(fastcp_common STATIC ${COMMON_SOURCES})

target_include_directories(fastcp_common PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/third_party/xxhash_repo
    ${CMAKE_SOURCE_DIR}/third_party/zstd_repo/lib
    ${CMAKE_SOURCE_DIR}/third_party/zstd_repo/lib/common
)

target_link_libraries(fastcp_common PUBLIC
    xxhash
    zstd_static
)

if(WIN32)
    target_link_libraries(fastcp_common PUBLIC ws2_32)
endif()

target_compile_features(fastcp_common PUBLIC cxx_std_17)

if(MSVC)
    target_compile_options(fastcp_common PRIVATE /W3 /wd4996 /wd4267 /wd4244 /utf-8)
else()
    target_compile_options(fastcp_common PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()
