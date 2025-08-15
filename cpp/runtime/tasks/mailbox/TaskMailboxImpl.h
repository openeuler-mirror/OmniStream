/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/22/25.
//

#ifndef OMNISTREAM_TASKMAILBOXIMPL_H
#define OMNISTREAM_TASKMAILBOXIMPL_H

#include <deque>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <optional>
#include <algorithm>
#include "TaskMailbox.h"


namespace omnistream {

    class TaskMailboxImpl : public TaskMailbox {
    public:
        explicit TaskMailboxImpl(std::thread::id taskMailboxThreadId) : taskMailboxThreadId(taskMailboxThreadId) {}
        TaskMailboxImpl() : taskMailboxThreadId(std::thread::id()) {}

        bool isMailboxThread() const override;
        bool hasMail() const override;
        int size();

        std::optional<std::shared_ptr<Mail>> tryTake(int priority) override;
        std::shared_ptr<Mail> take(int priority) override;

        bool createBatch() override;
        std::optional<std::shared_ptr<Mail>> tryTakeFromBatch() override;

        void put(std::shared_ptr<Mail>& mail) override;
        void putFirst(std::shared_ptr<Mail>& mail) override;

        std::vector<std::shared_ptr<Mail>> drain() override;
        void quiesce() override;
        std::vector<std::shared_ptr<Mail>> close() override;
        State getState()  override;
        void runExclusively(const std::shared_ptr<ThrowingRunnable>& runnable) override;

        std::string toString() override;

    private:
        std::recursive_mutex lock;
        std::deque<std::shared_ptr<Mail>> queue;
        std::condition_variable notEmpty;
        State state = State::OPEN;
        std::thread::id taskMailboxThreadId;
        std::deque<std::shared_ptr<Mail>> batch;
        std::atomic<bool> hasNewMail = false;

        std::shared_ptr<Mail> takeOrNull(std::deque<std::shared_ptr<Mail>>& queue, int priority);
        void checkIsMailboxThread() const;
        void checkPutStateConditions();
        void checkTakeStateConditions();
    };

} // namespace omnistream

#endif // OMNISTREAM_TASKMAILBOXIMPL_H