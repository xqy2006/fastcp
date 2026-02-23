FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    build-essential cmake git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /fastcp
COPY . .

# Remove the Windows build directory and any .git dirs in submodules
# (they may contain Windows-only object files / lock files)
RUN rm -rf build third_party/xxhash_repo/.git third_party/zstd_repo/.git

# Build
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build -j$(nproc)

CMD ["/bin/bash"]
