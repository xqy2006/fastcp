# ============================================================
# third_party/CMakeLists.txt
# Uses git-cloned xxHash and zstd repos directly
# ============================================================

# ---- xxhash ----
# Single .c + .h, no CMakeLists needed
add_library(xxhash STATIC
    xxhash/xxhash.c
)
target_include_directories(xxhash PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/xxhash
)
target_compile_definitions(xxhash PUBLIC
    XXH_STATIC_LINKING_ONLY
)
set_target_properties(xxhash PROPERTIES C_STANDARD 99)
if(MSVC)
    target_compile_options(xxhash PRIVATE /W0)
else()
    target_compile_options(xxhash PRIVATE -w)
endif()

# ---- zstd ----
# Collect all .c files from lib/ (common, compress, decompress, dictBuilder)
file(GLOB_RECURSE ZSTD_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib/common/*.c
    ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib/compress/*.c
    ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib/decompress/*.c
    ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib/dictBuilder/*.c
)
# Exclude deprecated & legacy
list(FILTER ZSTD_SOURCES EXCLUDE REGEX ".*/deprecated/.*")
list(FILTER ZSTD_SOURCES EXCLUDE REGEX ".*/legacy/.*")

add_library(zstd_static STATIC ${ZSTD_SOURCES})
target_include_directories(zstd_static PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib
    ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib/common
    ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib/compress
    ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib/decompress
    ${CMAKE_CURRENT_SOURCE_DIR}/zstd/lib/dictBuilder
)
target_compile_definitions(zstd_static PRIVATE
    ZSTD_DISABLE_ASM=1
    ZSTD_MULTITHREAD=0
)
set_target_properties(zstd_static PROPERTIES C_STANDARD 99)
if(MSVC)
    target_compile_options(zstd_static PRIVATE /W0)
else()
    target_compile_options(zstd_static PRIVATE -w)
endif()
