#pragma once

// ============================================================
// thread_pool.hpp -- Header-only C++17 thread pool
// ============================================================

#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <type_traits>

class ThreadPool {
public:
    explicit ThreadPool(size_t num_threads) : stop_(false) {
        if (num_threads == 0) num_threads = 1;
        workers_.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(mutex_);
                        cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
                        if (stop_ && tasks_.empty()) return;
                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    task();
                }
            });
        }
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
    }

    // Enqueue a callable and return a future for its result
    template<typename F, typename... Args>
    auto enqueue(F&& f, Args&&... args)
        -> std::future<typename std::invoke_result<F, Args...>::type>
    {
        using RetType = typename std::invoke_result<F, Args...>::type;

        auto task = std::make_shared<std::packaged_task<RetType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );

        std::future<RetType> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (stop_) throw std::runtime_error("ThreadPool is stopped");
            tasks_.emplace([task]() { (*task)(); });
        }
        cv_.notify_one();
        return res;
    }

    size_t size() const { return workers_.size(); }

    // Non-copyable, non-movable
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

private:
    std::vector<std::thread>          workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex                        mutex_;
    std::condition_variable           cv_;
    bool                              stop_;
};
