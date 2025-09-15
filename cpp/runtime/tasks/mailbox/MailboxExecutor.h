/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_MAILBOX_EXECUTOR_H
#define OMNISTREAM_MAILBOX_EXECUTOR_H

#include <any>
#include <memory>
#include <string>
#include <vector>
#include <future>
#include <exception>

#include "TaskMailbox.h"
#include "MailboxDefaultAction.h"
#include "StreamTaskActionExecutor.h"

namespace omnistream
{
    class MailboxExecutor
    {
    public:
        virtual ~MailboxExecutor() = default;

        virtual void execute(std::shared_ptr<ThrowingRunnable> command, const std::string& description) = 0;
        virtual void execute(std::shared_ptr<ThrowingRunnable> command, const std::string& descriptionFormat,
                             const std::vector<std::any>& descriptionArgs) = 0;

        virtual void yield() = 0;
        virtual bool tryYield() = 0;

        virtual std::string toString() const = 0;
    };
} // namespace omnistream

#endif // OMNISTREAM_MAILBOX_EXECUTOR_H
