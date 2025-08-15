/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/22/25.
//


#include <mutex>
#include <deque>
#include <condition_variable>
#include <thread>
#include <optional>
#include <vector>
#include <atomic>
#include <algorithm>

#include "TaskMailboxImpl.h"

namespace omnistream
{
    bool TaskMailboxImpl::isMailboxThread() const
    {
        // LOG("current id " << std::this_thread::get_id() << "and " << taskMailboxThreadId)
        return std::this_thread::get_id() == taskMailboxThreadId;
    }

    bool TaskMailboxImpl::hasMail() const
    {
        checkIsMailboxThread();
        return !batch.empty() || hasNewMail.load();
    }

    int TaskMailboxImpl::size()
    {
        std::lock_guard<std::recursive_mutex> lockGuard(lock);
        return static_cast<int>(batch.size() + queue.size());
    }

    std::optional<std::shared_ptr<Mail>> TaskMailboxImpl::tryTake(int priority)
    {
        checkIsMailboxThread();
        checkTakeStateConditions();
        std::shared_ptr<Mail> head = takeOrNull(batch, priority);
        if (head)
        {
            return head;
        }
        if (!hasNewMail.load())
        {
            return std::nullopt;
        }
        std::lock_guard<std::recursive_mutex> lockGuard(lock);
        head = takeOrNull(queue, priority);
        if (!head)
        {
            return std::nullopt;
        }
        hasNewMail.store(!queue.empty());
        return head;
    }

    std::shared_ptr<Mail> TaskMailboxImpl::take(int priority)
    {
        checkIsMailboxThread();
        checkTakeStateConditions();
        std::shared_ptr<Mail> head = takeOrNull(batch, priority);
        if (head)
        {
            return head;
        }
        std::lock_guard<std::recursive_mutex> lockGuard(lock);
        while (!(head = takeOrNull(queue, priority)))
        {
        }
        hasNewMail.store(!queue.empty());
        return head;
    }

    bool TaskMailboxImpl::createBatch()
    {
        checkIsMailboxThread();
        if (!hasNewMail.load())
        {
            return !batch.empty();
        }
        std::lock_guard<std::recursive_mutex> lockGuard(lock);
        auto mail = std::make_shared<Mail>();

        while (!queue.empty())
        {
            mail = queue.front();
            queue.pop_front();
            batch.push_back(mail);
        }
        hasNewMail.store(false);
        return !batch.empty();
    }

    std::optional<std::shared_ptr<Mail>> TaskMailboxImpl::tryTakeFromBatch()
    {
        checkIsMailboxThread();
        checkTakeStateConditions();
        if (batch.empty())
        {
            return std::nullopt;
        }
        std::shared_ptr<Mail> mail = batch.front();
        batch.pop_front();
        return mail;
    }

    void TaskMailboxImpl::put(std::shared_ptr<Mail>& mail)
    {
        std::lock_guard<std::recursive_mutex> lockGuard(lock);
        checkPutStateConditions();
        queue.push_back(mail);
        hasNewMail.store(true);
        notEmpty.notify_one();
    }

    void TaskMailboxImpl::putFirst(std::shared_ptr<Mail>& mail)
    {
        LOG("putFirst running")
        if (isMailboxThread())
        {
            checkPutStateConditions();
            batch.push_front(mail);
            LOG("putFirst  and put batch  mail " << mail->toString())
        }
        else
        {
            LOCK_BEFORE()
            std::lock_guard<std::recursive_mutex> lockGuard(lock);
            LOCK_AFTER()
            checkPutStateConditions();
            queue.push_front(mail);
            hasNewMail.store(true);
            notEmpty.notify_one();
            LOG("putFirst  and put queue  mail " << mail->toString())
        }
    }

    std::shared_ptr<Mail> TaskMailboxImpl::takeOrNull(std::deque<std::shared_ptr<Mail>>& queue, int priority)
    {
        auto it = std::find_if(queue.begin(), queue.end(), [priority](const std::shared_ptr<Mail>& mail)
        {
            return mail->getPriority() >= priority;
        });
        if (it != queue.end())
        {
            std::shared_ptr<Mail> mail = *it;
            queue.erase(it);
            return mail;
        }
        return nullptr;
    }

    std::vector<std::shared_ptr<Mail>> TaskMailboxImpl::drain()
    {
        std::vector<std::shared_ptr<Mail>> drainedMails(batch.begin(), batch.end());
        batch.clear();
        std::lock_guard<std::recursive_mutex> lockGuard(lock);
        drainedMails.insert(drainedMails.end(), queue.begin(), queue.end());
        queue.clear();
        hasNewMail.store(false);
        return drainedMails;
    }

    void TaskMailboxImpl::checkIsMailboxThread() const
    {
        // LOG("checkIsMailboxThread running")
        if (!isMailboxThread())
        {
            throw std::runtime_error(
                    "Illegal thread detected. This method must be called from inside the mailbox thread!");
        }
    }

    void TaskMailboxImpl::checkPutStateConditions()
    {
        if (state != State::OPEN)
        {
            throw MailboxClosedException(
                    "Mailbox is in state " + std::to_string(static_cast<int>(state)) + ", but is required to be in state " +
                    std::to_string(static_cast<int>(State::OPEN)) + " for put operations.");
        }
    }

    void TaskMailboxImpl::checkTakeStateConditions()
    {
        if (state == State::CLOSED)
        {
            throw MailboxClosedException(
                    "Mailbox is in state " + std::to_string(static_cast<int>(state)) + ", but is required to be in state " +
                    std::to_string(static_cast<int>(State::OPEN)) + " or " + std::to_string(
                            static_cast<int>(State::QUIESCED)) + " for take operations.");
        }
    }

    void TaskMailboxImpl::quiesce()
    {
        checkIsMailboxThread();
        std::lock_guard<std::recursive_mutex> lockGuard(lock);
        if (state == State::OPEN)
        {
            state = State::QUIESCED;
        }
    }

    std::vector<std::shared_ptr<Mail>> TaskMailboxImpl::close()
    {
        checkIsMailboxThread();
        std::lock_guard<std::recursive_mutex> lockGuard(lock);
        if (state == State::CLOSED)
        {
            return {};
        }
        std::vector<std::shared_ptr<Mail>> droppedMails = drain();
        state = State::CLOSED;
        notEmpty.notify_all();
        return droppedMails;
    }

    TaskMailbox::State TaskMailboxImpl::getState()
    {
        LOG("getState running")
        if (isMailboxThread())
        {
            return state;
        }
        std::lock_guard<std::recursive_mutex> lockGuard(lock);
        return state;
    }

    void TaskMailboxImpl::runExclusively(const std::shared_ptr<ThrowingRunnable>& runnable)
    {
        LOG("runExclusively running")
        std::lock_guard<std::recursive_mutex> lockGuard(lock);
        runnable->run();
    }

    std::string TaskMailboxImpl::toString()  {
        std::stringstream ss;
        ss << "TaskMailboxImpl {" << std::endl;
        ss << "  taskMailboxThreadId: " << taskMailboxThreadId << std::endl;
        ss << "  state: " << static_cast<int>(state) << std::endl;
        ss << "  queue: [" << std::endl;
        for (const auto& mail : queue) {
            if (mail) {
                ss << "    " << mail->toString() << "," << std::endl;
            } else {
                ss << "    nullptr," << std::endl;
            }
        }
        ss << "  ]" << std::endl;
        ss << "  batch: [" << std::endl;
        for (const auto& mail : batch) {
            if (mail) {
                ss << "    " << mail->toString() << "," << std::endl;
            } else {
                ss << "    nullptr," << std::endl;
            }
        }
        ss << "  ]" << std::endl;
        ss << "  hasNewMail: " << hasNewMail.load() << std::endl;
        ss << "}";
        return ss.str();
    }
} // namespace omnistream
