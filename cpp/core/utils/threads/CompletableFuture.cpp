/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "CompletableFuture.h"
#include <iostream>

namespace omnistream {

CompletableFuture::CompletableFuture()
    : state(FutureState::NOT_STARTED), done(false) {
}

CompletableFuture::~CompletableFuture()
{
    if (worker.joinable()) {
        worker.join();
    }
}

CompletableFuture::CompletableFuture(CompletableFuture&& other) noexcept
    : state(other.state), done(other.done.load()), exception(std::move(other.exception))
{
    if (other.worker.joinable()) {
        other.worker.detach();
    }
}

CompletableFuture& CompletableFuture::operator=(CompletableFuture&& other) noexcept
{
    if (this != &other) {
        if (worker.joinable()) {
            worker.join();
        }

        state = other.state;
        done.store(other.done.load());
        exception = std::move(other.exception);

        if (other.worker.joinable()) {
            other.worker.detach();
        }
    }
    return *this;
}

void CompletableFuture::executeTask(CompletableFuture* future, Runnable* task)
{
    try {
        {
            std::lock_guard<std::mutex> lock(future->mtx);
            future->state = FutureState::RUNNING;
        }

        task->run();

        {
            std::lock_guard<std::mutex> lock(future->mtx);
            future->state = FutureState::COMPLETED;
        }
    } catch (...) {
        std::lock_guard<std::mutex> lock(future->mtx);
        future->exception = std::current_exception();
        future->state = FutureState::FAILED;
    }

    future->done.store(true);
    future->cv.notify_all();
}

void CompletableFuture::runAs(Runnable* task)
{
    if (isDone()) {
        throw std::runtime_error("Future already completed");
    }

    {
        std::lock_guard<std::mutex> lock(mtx);
        if (state != FutureState::NOT_STARTED) {
            throw std::runtime_error("Future already started");
        }
    }

    worker = std::thread(executeTask, this, task);
}

std::shared_ptr<CompletableFuture> CompletableFuture::runAsync(Runnable* task)
{
    std::shared_ptr<CompletableFuture> future = std::make_shared<CompletableFuture>();
    future->runAs(task);
    return future;
}

std::shared_ptr<CompletableFuture> CompletableFuture::thenRun(Runnable* task)
{
    class ChainedTask : public Runnable {
    public:
        ChainedTask(CompletableFuture* parentFuture, Runnable* nextTask)
            : parent(parentFuture), next(nextTask) {}

        void run() override
        {
            // Wait for parent to complete
            parent->get();

            // Run the next task
            next->run();
        }
    private:
        CompletableFuture* parent;
        Runnable* next;
    };

    ChainedTask* chainedTask = new ChainedTask(this, task);
    return runAsync(chainedTask);
}

FutureState CompletableFuture::getState()
{
    std::lock_guard<std::mutex> lock(mtx);
    return state;
}

void CompletableFuture::get()
{
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this] { return done.load(); });

    if (exception) {
        std::rethrow_exception(exception);
    }
}

bool CompletableFuture::get(long timeoutMillis)
{
    std::unique_lock<std::mutex> lock(mtx);
    bool result = cv.wait_for(lock, std::chrono::milliseconds(timeoutMillis), [this] { return done.load(); });
    if (result && exception) {
        std::rethrow_exception(exception);
    }

    return result;
}

std::shared_ptr<CompletableFuture> CompletableFuture::allOf(const std::vector<std::shared_ptr<CompletableFuture>>& futures)
{
    class AllOfTask : public Runnable {
    public:
        explicit AllOfTask(const std::vector<std::shared_ptr<CompletableFuture>>& futures)
            : allFutures(futures) {}

        void run() override
        {
            for (const auto& future : allFutures) {
                future->get();
            }
        }
    private:
        std::vector<std::shared_ptr<CompletableFuture>> allFutures;
    };

    AllOfTask* allOfTask = new AllOfTask(futures);
    return runAsync(allOfTask);
}

std::shared_ptr<CompletableFuture> CompletableFuture::anyOf(const std::vector<std::shared_ptr<CompletableFuture>>& futures)
{
    class AnyOfTask : public Runnable {
    public:
        explicit AnyOfTask(const std::vector<std::shared_ptr<CompletableFuture>>& futures)
            : anyFutures(futures) {}

        void run() override
        {
            std::atomic<bool> anyDone(false);
            std::vector<std::thread> checkers;

            for (const auto& future : anyFutures) {
                checkers.emplace_back([future, &anyDone]() {
                    try {
                        future->get();
                    } catch (...) {
                        // Ignore exceptions, just mark as done
                    }
                    anyDone.store(true);
                });
            }

            // Wait for any future to complete or throw
            while (!anyDone.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            // Detach remaining threads
            for (auto& thread : checkers) {
                if (thread.joinable()) {
                    thread.detach();
                }
            }
        }
    private:
        std::vector<std::shared_ptr<CompletableFuture>> anyFutures;
    };

    AnyOfTask* anyOfTask = new AnyOfTask(futures);
    return runAsync(anyOfTask);
}

bool CompletableFuture::cancel()
{
    std::lock_guard<std::mutex> lock(mtx);

    if (done.load() || state == FutureState::RUNNING) {
        return false;
    }

    state = FutureState::CANCELLED;
    done.store(true);
    cv.notify_all();

    return true;
}

bool CompletableFuture::isDone() const
{
    return done.load();
}

bool CompletableFuture::isCancelled()
{
    std::lock_guard<std::mutex> lock(mtx);
    return state == FutureState::CANCELLED;
}

void CompletableFuture::setCompleted()
{
    std::lock_guard<std::mutex> lock(mtx);
    state = FutureState::COMPLETED;
}

std::string CompletableFuture::toString() const
{
    switch (state) {
        case FutureState::CANCELLED:
            return "Cancelled";
        case FutureState::NOT_STARTED:
            return "NotStarted";
        case FutureState::RUNNING:
            return "Running";
        case FutureState::COMPLETED:
            return "Completed";
        case FutureState::FAILED:
            return "Failed";
    }
    return {};
}

} // namespace omnistream