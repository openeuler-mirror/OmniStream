/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/22/25.
//


#ifndef TASKMAILBOX_H
#define TASKMAILBOX_H

#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include "Mail.h"
#include "core/utils/function/ThrowingRunnable.h"

namespace omnistream {

    class TaskMailbox {
    public:
        static constexpr int MIN_PRIORITY = -1;
        static constexpr int MAX_PRIORITY = std::numeric_limits<int>::max();

        enum class State { OPEN, QUIESCED, CLOSED };

        virtual ~TaskMailbox() = default;

        virtual bool isMailboxThread() const = 0;
        virtual bool hasMail() const = 0;
        virtual std::optional<std::shared_ptr<Mail>> tryTake(int priority) = 0;
        virtual std::shared_ptr<Mail> take(int priority) = 0;

        virtual bool createBatch() = 0;
        virtual std::optional<std::shared_ptr<Mail>> tryTakeFromBatch() = 0;

        virtual void put( std::shared_ptr<Mail>& mail) = 0;
        virtual void putFirst( std::shared_ptr<Mail>& mail) = 0;

        virtual std::vector<std::shared_ptr<Mail>> drain() = 0;
        virtual void quiesce() = 0;
        virtual std::vector<std::shared_ptr<Mail>> close() = 0;
        virtual State getState()  = 0;

        virtual void runExclusively(const std::shared_ptr<ThrowingRunnable>& runnable) = 0;

        virtual std::string toString() = 0;

        class MailboxClosedException: public std::runtime_error {
        public:
            explicit MailboxClosedException(const std::string& message)
              : std::runtime_error(message) {}
        };
    };

}  // namespace omnistream

#endif  // TASKMAILBOX_H