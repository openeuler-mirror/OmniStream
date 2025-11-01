/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#include "ChannelStateWriteRequestExecutorImpl.h"

namespace omnistream {

    ChannelStateWriteRequestExecutorImpl::ChannelStateWriteRequestExecutorImpl(
        ChannelStateWriteRequestDispatcher *dispatcher,
        int maxSubtasks,
        std::function<void(ChannelStateWriteRequestExecutor *)> onRegistered,
        std::mutex &registerLock)
        : dispatcher(dispatcher),
          maxSubtasks(maxSubtasks),
          onRegistered(std::move(onRegistered)),
          registerLock(registerLock),
          isRegistering(true),
          stopped(false),
          started(false) {}

    ChannelStateWriteRequestExecutorImpl::~ChannelStateWriteRequestExecutorImpl()
    {
        shutdown();
        if (worker.joinable()) {
            worker.join();
        }
    }

    void ChannelStateWriteRequestExecutorImpl::start()
    {
        if (!started.exchange(true)) {
            worker = std::thread([this] { run(); });
        }
    }

    void ChannelStateWriteRequestExecutorImpl::submit(std::unique_ptr<ChannelStateWriteRequest> req)
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!stopped) {
            enqueue(std::move(req), false);
        }
    }

    void ChannelStateWriteRequestExecutorImpl::submitPriority(std::unique_ptr<ChannelStateWriteRequest> req)
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!stopped) {
            enqueue(std::move(req), true);
        }
    }

    void ChannelStateWriteRequestExecutorImpl::registerSubtask(const JobVertexID &jvid, int idx)
    {
        std::lock_guard<std::mutex> regLock(registerLock);
        std::lock_guard<std::mutex> lock(mutex);
        if (!isRegistering || stopped) {
            throw std::logic_error("registration already completed or executor stopped");
        }

        SubtaskID sid = SubtaskID::Of(jvid, idx);
        subtasks.insert(sid);
        unreadyQueues.emplace(sid, std::queue<std::unique_ptr<ChannelStateWriteRequest>>());

        enqueue(ChannelStateWriteRequest::registerSubtask(jvid, idx), false);

        if ((int)subtasks.size() >= maxSubtasks) {
            completeRegistration();
        }
    }

    void ChannelStateWriteRequestExecutorImpl::releaseSubtask(const JobVertexID &jvid, int idx)
    {
        std::lock_guard<std::mutex> regLock(registerLock);
        std::lock_guard<std::mutex> lock(mutex);
        SubtaskID sid = SubtaskID::Of(jvid, idx);

        enqueue(ChannelStateWriteRequest::releaseSubtask(jvid, idx), false);
        subtasks.erase(sid);

        if (subtasks.empty()) {
            stopped = true;
        }
        cv.notify_all();
    }

    void ChannelStateWriteRequestExecutorImpl::shutdown()
    {
        std::lock_guard<std::mutex> lock(mutex);
        stopped = true;
        cv.notify_all();
    }

    void ChannelStateWriteRequestExecutorImpl::run()
    {
        try {
            loop();
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex);
            exceptionPtr = std::current_exception();
        }

        cleanup();
    }

    void ChannelStateWriteRequestExecutorImpl::loop()
    {
        while (true) {
            auto req = take();
            if (!req) {
                return;
            }

            if (dynamic_cast<CheckpointStartRequest *>(req.get())) {
                std::lock_guard<std::mutex> regLock(registerLock);
                if (isRegistering.exchange(false)) {
                    onRegistered(this);
                }
            }

            dispatcher->dispatch(*req);
        }
    }

    std::unique_ptr<ChannelStateWriteRequest> ChannelStateWriteRequestExecutorImpl::take()
    {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [&]() { return stopped || !readyQueue.empty(); });
        if (readyQueue.empty()) {
            return nullptr;
        }
        std::unique_ptr<ChannelStateWriteRequest> req = std::move(readyQueue.front());
        readyQueue.pop_front();
        return req;
    }

    void ChannelStateWriteRequestExecutorImpl::enqueue(std::unique_ptr<ChannelStateWriteRequest> req, bool priority)
    {
        SubtaskID sid = SubtaskID::Of(req->getJobVertexID(), req->getSubtaskIndex());
        auto &queue = unreadyQueues[sid];

        if (!queue.empty() || !req->getReadyFuture()->IsDone()) {
            ChannelStateWriteRequest *raw = req.get();
            queue.push(std::move(req));

            registerCallback(raw, sid);
        } else {
            if (priority) {
                readyQueue.push_front(std::move(req));
            } else {
                readyQueue.push_back(std::move(req));
            }
            cv.notify_all();
        }
    }

    void ChannelStateWriteRequestExecutorImpl::registerCallback(ChannelStateWriteRequest *first, SubtaskID sid)
    {
        auto future = first->getReadyFuture();
        future->ThenRun([this, first, sid, future]() {
            std::lock_guard<std::mutex> lock(mutex);
            auto it = unreadyQueues.find(sid);
            if (it == unreadyQueues.end()) {
                return;
            }
            auto& q = it->second;

            while (!q.empty() && q.front()->getReadyFuture()->IsDone()) {
                readyQueue.push_back(std::move(q.front()));
                q.pop();
            }
            if (!q.empty()) {
                registerCallback(q.front().get(), sid);
            }
            cv.notify_all();
        });
    }

    void ChannelStateWriteRequestExecutorImpl::cleanup()
    {
        std::vector<std::unique_ptr<ChannelStateWriteRequest>> all;
        {
            std::lock_guard<std::mutex> lock(mutex);

            while (!readyQueue.empty()) {
                all.push_back(std::move(readyQueue.front()));
                readyQueue.pop_front();
            }

            for (auto &kv : unreadyQueues) {
                auto &q = kv.second;
                while (!q.empty()) {
                    all.push_back(std::move(q.front()));
                    q.pop();
                }
            }
            unreadyQueues.clear();
        }

        auto cause = exceptionPtr ? exceptionPtr : std::make_exception_ptr(std::runtime_error("executor stopped"));

        for (auto &r : all) {
            r->cancel(cause);
        }

        if (exceptionPtr) {
            dispatcher->fail(exceptionPtr);
        }
    }

    void ChannelStateWriteRequestExecutorImpl::completeRegistration()
    {
        if (isRegistering.exchange(false)) {
            onRegistered(this);
        }
    }

} // namespace omnistream
