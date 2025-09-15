/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef STREAM_TASK_ACTION_EXECUTOR_H
#define STREAM_TASK_ACTION_EXECUTOR_H

#include <string>
#include <exception>
#include <memory>
#include <mutex>
#include <sstream>
#include "core/utils/function/ThrowingRunnable.h"

namespace omnistream
{
    class StreamTaskActionExecutor
    {
    public:
        virtual ~StreamTaskActionExecutor() = default;

        virtual void run(ThrowingRunnable* const runnable) = 0;

        virtual std::string toString() const
        {
            return "StreamTaskActionExecutor";
        }

        static std::shared_ptr<StreamTaskActionExecutor> IMMEDIATE;
    };


    class ImmediateStreamTaskActionExecutor : public StreamTaskActionExecutor
    {
    public:
        ImmediateStreamTaskActionExecutor() = default;
        ~ImmediateStreamTaskActionExecutor() override = default;

        void run(ThrowingRunnable* const runnable) override
        {
            runnable->run();
        }

        std::string toString() const override
        {
            return "ImmediateStreamTaskActionExecutor";
        }
    };


    class SynchronizedStreamTaskActionExecutor : public StreamTaskActionExecutor
    {
    private:
        std::shared_ptr<std::mutex> mutex_;

    public:
        SynchronizedStreamTaskActionExecutor() = default;

        explicit SynchronizedStreamTaskActionExecutor(std::shared_ptr<std::mutex> mutex) : mutex_(mutex)
        {
        };
        ~SynchronizedStreamTaskActionExecutor() override = default;

        void run(ThrowingRunnable* runnable) override
        {
            std::lock_guard<std::mutex> lock(*mutex_);
            runnable->run();
        }

        std::shared_ptr<std::mutex> getMutex()
        {
            return mutex_;
        }

        std::string toString() const override
        {
            std::stringstream ss;
            ss << "SynchronizedStreamTaskActionExecutor";
            return ss.str();
        }
    };

    inline std::shared_ptr<SynchronizedStreamTaskActionExecutor> synchronizedExecutor()
    {
        return std::make_shared<SynchronizedStreamTaskActionExecutor>();
    }

    inline std::shared_ptr<SynchronizedStreamTaskActionExecutor> synchronizedExecutor(std::shared_ptr<std::mutex> mutex)
    {
        return std::make_shared<SynchronizedStreamTaskActionExecutor>(mutex);
    }
} // namespace omnistream

#endif // STREAM_TASK_ACTION_EXECUTOR_H
