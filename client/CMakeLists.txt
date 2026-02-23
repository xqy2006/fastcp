set(CLIENT_SOURCES
    session_manager.cpp
    file_receiver.cpp
    connection_handler.cpp
    client_app.cpp
    main.cpp
)

add_executable(fastcp_client ${CLIENT_SOURCES})

target_include_directories(fastcp_client PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/common
)

target_link_libraries(fastcp_client PRIVATE fastcp_common)

if(WIN32)
    target_link_libraries(fastcp_client PRIVATE ws2_32)
endif()

target_compile_features(fastcp_client PRIVATE cxx_std_17)

if(MSVC)
    target_compile_options(fastcp_client PRIVATE /W3 /wd4996 /utf-8)
else()
    target_compile_options(fastcp_client PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()

set_target_properties(fastcp_client PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
