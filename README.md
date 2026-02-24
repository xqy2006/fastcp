# fastcp

一个用 C++17 编写的高速多连接文件同步工具。通过 TCP 将目录从server同步到一个或多个client，支持并行连接、内存映射 I/O、流水线同步、块级断点续传、Delta 增量同步和完整性校验。

---

## 快速开始

```bash
# 终端 1 — 服务器（发送方，持久守护进程）
fastcp_server /opt/data 0.0.0.0 9999

# 终端 2 — 客户端（接收方）
fastcp_client /home/user/data 192.168.1.1 9999
```

先启动服务器。客户端支持指数退避重试连接，可以在服务器启动前运行（`--retry N`）。多个客户端可以同时连接。

---

## 接口说明

```
fastcp_server <src_dir> <ip> <port> [选项]
fastcp_client <dst_dir> <ip> <port> [选项]
```

| 角色       | 可执行文件      | 职责                                                         |
| ---------- | --------------- | ------------------------------------------------------------ |
| **server** | `fastcp_server` | 持久守护进程：绑定 `ip:port`，启动时扫描 `src_dir` 一次；同时服务多个并发客户端会话 |
| **client** | `fastcp_client` | 连接服务器；打开 N 个并行 TCP 连接；将文件接收/同步到 `dst_dir` |

服务器端的 `<ip>` 是**监听地址**（如 `0.0.0.0` 表示所有接口）。

### 服务器选项

| 选项            | 默认值 | 说明                                    |
| --------------- | ------ | --------------------------------------- |
| `--conns N`     | 4      | 每个客户端期望的并行 TCP 连接数（1–64） |
| `--no-compress` | 关     | 禁用 zstd 压缩                          |
| `--chunk-kb N`  | 1024   | 大文件分块大小（KB），范围 4–65536      |
| `--verbose`     | 关     | 启用调试日志                            |

### 客户端选项

| 选项        | 默认值 | 说明                           |
| ----------- | ------ | ------------------------------ |
| `--conns N` | 4      | 打开的并行 TCP 连接数（1–64）  |
| `--retry N` | 30     | 服务器未就绪时的重试时间（秒） |
| `--verbose` | 关     | 启用调试日志                   |

### 使用示例

```bash
# 基本用法
fastcp_server /opt/var 0.0.0.0 9999
fastcp_client /home/alice/data 192.168.1.1 9999

# 多客户端同时运行
fastcp_client /home/bob/data   192.168.1.1 9999
fastcp_client /home/carol/data 192.168.1.1 9999

# 客户端先于服务器启动（重试最多 60 秒）
fastcp_client /home/user/data 192.168.1.1 9999 --retry 60

# 8 个并行连接，禁用压缩
fastcp_server /opt/var 0.0.0.0 9999 --conns 8 --no-compress
fastcp_client /home/user/data 192.168.1.1 9999 --conns 8

# 256 KB 分块（适合细粒度断点续传）
fastcp_server /opt/var 0.0.0.0 9999 --chunk-kb 256
fastcp_client /home/user/data 192.168.1.1 9999

# Windows
fastcp_server.exe C:\share 0.0.0.0 9999 --conns 4
fastcp_client.exe D:\data  192.168.1.1 9999 --conns 4
```

---

## 构建

### 依赖要求

| 工具       | 最低版本                               |
| ---------- | -------------------------------------- |
| CMake      | 3.16                                   |
| C++ 编译器 | GCC 11 / Clang 13 / MSVC 2019（C++17） |

第三方库（xxHash、zstd）已内置于 `third_party/`，无需手动下载。

### Linux / macOS

```bash
git clone https://github.com/yourname/fastcp.git
cd fastcp
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

### Windows（MSVC）

```powershell
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release -j8
```

### Docker

```bash
docker build -t fastcp:linux .
docker run --rm fastcp:linux /fastcp/build/bin/fastcp_server --help
```



### 协议

所有通信使用基于 TCP 的二进制帧协议：

```
+----------+----------+-------------+
| msg_type | flags    | payload_len |   ← 8 字节帧头（大端序）
|  2 字节  | 2 字节   |   4 字节    |
+----------+----------+-------------+
|               payload             |   ← 可变长度
+-----------------------------------+
```

握手期间协商功能位（`capabilities` 位掩码）：

| 位     | 功能                  | 说明                              |
| ------ | --------------------- | --------------------------------- |
| 0x0001 | `CAP_COMPRESS`        | 逐块 zstd 压缩                    |
| 0x0002 | `CAP_RESUME`          | 文件级偏移断点续传                |
| 0x0004 | `CAP_BUNDLE`          | 小文件打包传输                    |
| 0x0008 | `CAP_DELTA`           | rsync 风格块级 Delta 同步         |
| 0x0010 | `CAP_VIRTUAL_ARCHIVE` | 虚拟字节流                        |
| 0x0020 | `CAP_CHUNK_RESUME`    | 块级哈希断点续传                  |
| 0x0040 | `CAP_PIPELINE_SYNC`   | 流水线文件树推送 + WANT_FILE 拉取 |

全功能协商后位掩码为 `0x007e`。
