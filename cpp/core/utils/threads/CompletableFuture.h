/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>
#include <chrono>
#include <memory>
#include <exception>

namespace omnistream {

// Forward declaration
class CompletableFuture;

// Runnable interface that all tasks must implement
class Runnable {
public:
    virtual ~Runnable() = default;
    virtual void run() = 0;
};

// Task wrapper for internal scheduling
    // for extension such as logging
class Task : public Runnable {
public:
    explicit Task(Runnable* runnable) : target(runnable) {}

    void run() override {
        if (target) {
            target->run();
        }
    }
private:
    Runnable* target;

};

enum class FutureState {
    NOT_STARTED,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
};

class CompletableFuture {
private:
    FutureState state;
    std::atomic<bool> done;
    std::exception_ptr exception;
    std::mutex mtx;
    std::condition_variable cv;
    std::thread worker;

    // Static method for thread execution
    static void executeTask(CompletableFuture* future, Runnable* task);

public:
    CompletableFuture();
    ~CompletableFuture();

    // Prevent copying
    CompletableFuture(const CompletableFuture&) = delete;
    CompletableFuture& operator=(const CompletableFuture&) = delete;

    // Allow moving
    CompletableFuture(CompletableFuture&&) noexcept;
    CompletableFuture& operator=(CompletableFuture&&) noexcept;

    // Submit a task to run synchronously
    void runAs(Runnable* task);

    // Run a task asynchronously and return a new CompletableFuture
    static std::shared_ptr<CompletableFuture> runAsync(Runnable* task);

    // Chain a task to run after this one completes
    std::shared_ptr<CompletableFuture> thenRun(Runnable* task);

    // Check the current state
    FutureState getState() ;

    // Wait for completion and get the result
    void get();

    // Wait for completion with timeout
    bool get(long timeoutMillis);

    // Static methods for combining futures
    static std::shared_ptr<CompletableFuture> allOf(const std::vector<std::shared_ptr<CompletableFuture>>& futures);
    static std::shared_ptr<CompletableFuture> anyOf(const std::vector<std::shared_ptr<CompletableFuture>>& futures);

    // Cancel the task
    bool cancel();

    // Check if task is done
    bool isDone() const;

    // Check if task was cancelled
    bool isCancelled() ;

    void setCompleted() ;

    std::string toString() const;
};

} // namespace omnistream
