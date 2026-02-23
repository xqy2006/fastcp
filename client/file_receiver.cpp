// ============================================================
// file_receiver.cpp
// ============================================================

#include "file_receiver.hpp"
#include "../common/compress.hpp"
#include "../common/utils.hpp"
#include <cstring>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

bool FileReceiver::on_file_meta(const FileMeta& meta, const std::string& rel_path) {
    try {
        fs::path abs = file_io::proto_to_fspath(session_->root_dir, rel_path);
        file_io::ensure_parent_dirs(abs.string());

        auto state = std::make_unique<FileReceiveState>();
        state->file_id      = meta.file_id;
        state->file_size    = meta.file_size;
        state->mtime_ns     = meta.mtime_ns;
        state->chunk_count  = meta.chunk_count;
        state->chunk_size   = meta.chunk_size;
        state->compress_algo = meta.compress_algo;
        state->abs_path     = abs.string();
        state->rel_path     = rel_path;
        std::memcpy(state->expected_hash, meta.xxh3, 16);

        // If transfer_index says we already have partial data, open in resume mode
        // (don't truncate the existing partial file).
        // Also open in resume mode if we sent block checksums for this file
        // (delta sync: server will only send changed blocks, client keeps the rest).
        u64 already_received = session_->transfer_index->get_received(meta.file_id);
        bool resume = (already_received > 0 && already_received < meta.file_size);
        if (!resume) {
            std::lock_guard<std::mutex> lk(session_->delta_sent_mutex);
            if (session_->delta_sent.count(meta.file_id)) {
                resume = true; // preserve existing data; server will overwrite changed blocks
            }
        }
        state->writer.open(abs.string(), meta.file_size, resume);

        {
            std::lock_guard<std::mutex> lk(states_mutex_);
            states_[meta.file_id] = std::move(state);
        }

        LOG_INFO("Receiving file: " + rel_path + " (" +
                 utils::format_bytes(meta.file_size) + ")");
        return true;
    } catch (const std::exception& e) {
        LOG_ERROR("on_file_meta: " + std::string(e.what()));
        return false;
    }
}

int FileReceiver::on_file_chunk(const FileChunk& chunk, const u8* compressed_data) {
    FileReceiveState* state = get_state(chunk.file_id);
    if (!state) {
        LOG_ERROR("on_file_chunk: unknown file_id=" + std::to_string(chunk.file_id));
        return -1;
    }

    try {
        const u8* write_data = compressed_data;
        u32 write_len = chunk.data_len;
        std::vector<u8> decomp_buf;

        // Decompress if this specific chunk was compressed (pad[0] == 1)
        if (chunk.pad[0] == 1) {
            u64 raw_len = state->chunk_size;
            // Last chunk may be smaller
            u64 offset_end = chunk.file_offset + state->chunk_size;
            if (offset_end > state->file_size) {
                raw_len = state->file_size - chunk.file_offset;
            }
            decomp_buf = compress::decompress_to_vec(
                compressed_data, chunk.data_len, (size_t)raw_len);
            write_data = decomp_buf.data();
            write_len  = (u32)decomp_buf.size();
        }

        // Verify per-chunk hash (always against raw data)
        u32 computed = hash::xxh3_32(write_data, write_len);
        if (computed != chunk.xxh3_32) {
            LOG_WARN("Chunk hash mismatch: file_id=" + std::to_string(chunk.file_id) +
                     " chunk=" + std::to_string(chunk.chunk_index));
            return (int)chunk.chunk_index;
        }

        // Write to mmap
        state->writer.write_at(chunk.file_offset, write_data, write_len);

        // Update incremental hash
        {
            std::lock_guard<std::mutex> lk(state->hasher_mutex);
            state->hasher.update(write_data, write_len);
        }

        state->bytes_received.fetch_add(write_len);
        state->chunks_received.fetch_add(1);
        session_->bytes_received.fetch_add(write_len);

        // Update transfer index for resume support
        session_->transfer_index->set_received(
            chunk.file_id, state->bytes_received.load());

        return 0;
    } catch (const std::exception& e) {
        LOG_ERROR("on_file_chunk: " + std::string(e.what()));
        return -1;
    }
}

bool FileReceiver::on_file_end(const FileEnd& end_msg) {
    FileReceiveState* state = get_state(end_msg.file_id);
    if (!state) {
        LOG_ERROR("on_file_end: unknown file_id=" + std::to_string(end_msg.file_id));
        return false;
    }

    // Close mmap writer first (flush to disk)
    state->writer.close(state->mtime_ns);

    // Re-read the written file to verify full hash (handles out-of-order multi-conn chunks)
    bool hash_ok = false;
    try {
        if (state->file_size == 0) {
            auto digest = hash::xxh3_128(nullptr, 0);
            hash_ok = (std::memcmp(digest.data(), end_msg.xxh3_128, 16) == 0);
        } else {
            file_io::MmapReader verify_reader(state->abs_path);
            auto digest = hash::xxh3_128(verify_reader.data(), (size_t)verify_reader.size());
            hash_ok = (std::memcmp(digest.data(), end_msg.xxh3_128, 16) == 0);
        }
    } catch (const std::exception& e) {
        LOG_ERROR("Hash verify read failed: " + std::string(e.what()));
        hash_ok = false;
    }

    if (!hash_ok) {
        LOG_ERROR("File hash mismatch: " + state->rel_path);
        return false;
    }

    state->complete = true;

    // Remove from transfer index (done)
    session_->transfer_index->remove(end_msg.file_id);
    session_->files_done.fetch_add(1);

    LOG_INFO("File complete: " + state->rel_path);

    // Remove from active states
    {
        std::lock_guard<std::mutex> lk(states_mutex_);
        states_.erase(end_msg.file_id);
    }
    return true;
}

bool FileReceiver::on_bundle_entry(const BundleEntryHdr& hdr,
                                    const std::string& rel_path,
                                    const u8* data, u64 data_len)
{
    try {
        fs::path abs = file_io::proto_to_fspath(session_->root_dir, rel_path);
        file_io::ensure_parent_dirs(abs.string());

        // Verify hash
        auto digest = hash::xxh3_128(data, (size_t)data_len);
        if (std::memcmp(digest.data(), hdr.xxh3, 16) != 0) {
            LOG_ERROR("Bundle entry hash mismatch: " + rel_path);
            return false;
        }

        // Write file
        {
            file_io::MmapWriter writer;
            writer.open(abs.string(), data_len);
            writer.write_at(0, data, (size_t)data_len);
            writer.close(hdr.mtime_ns);
        }

        session_->bytes_received.fetch_add(data_len);
        session_->files_done.fetch_add(1);
        return true;
    } catch (const std::exception& e) {
        LOG_ERROR("on_bundle_entry: " + std::string(e.what()));
        return false;
    }
}

FileReceiveState* FileReceiver::get_state(u32 file_id) {
    std::lock_guard<std::mutex> lk(states_mutex_);
    auto it = states_.find(file_id);
    return it != states_.end() ? it->second.get() : nullptr;
}
