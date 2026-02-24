set(SERVER_SOURCES
    dir_scanner.cpp
    connection_pool.cpp
    file_sender.cpp
    archive_builder.cpp
    server_app.cpp
    main.cpp
)

add_executable(fastcp_server ${SERVER_SOURCES})

target_include_directories(fastcp_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/common
)

target_link_libraries(fastcp_server PRIVATE fastcp_common)

if(WIN32)
    target_link_libraries(fastcp_server PRIVATE ws2_32)
endif()

target_compile_features(fastcp_server PRIVATE cxx_std_17)

if(MSVC)
    target_compile_options(fastcp_server PRIVATE /W3 /wd4996 /utf-8)
else()
    target_compile_options(fastcp_server PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()

set_target_properties(fastcp_server PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
