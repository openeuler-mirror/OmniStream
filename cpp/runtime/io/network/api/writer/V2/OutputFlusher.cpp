/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#include "OutputFlusher.h"
#include "RecordWriterV2.h"
#include <chrono>
#include <stdexcept>
#include <iostream>

namespace omnistream {

OutputFlusher::OutputFlusher(const std::string& name, long timeout, RecordWriterV2* writer)
    : timeout(timeout),
      writer(writer),
      running(false),
      taskName(name)
{
}

OutputFlusher::~OutputFlusher()
{
    INFO_RELEASE("~OutputFlusher for " << taskName);
    terminate();
    if (thread.joinable()) {
        thread.join();
    }
}

void OutputFlusher::terminate()
{
    // INFO_RELEASE, not LOG_INFO_IMP: that macro is disabled in this build, which is why the
    // terminate/terminated messages were invisible while diagnosing the thread leak.
    INFO_RELEASE("OutputFlusher::terminate entered for " << taskName);
    running = false;
    if (thread.joinable()) {
        thread.join();
    }
    INFO_RELEASE("OutputFlusher::terminate joined for " << taskName);
}

void OutputFlusher::start()
{
    if (thread.joinable()) {
        throw std::runtime_error("OutputFlusher already started.");
    }
    running = true;
    thread = std::thread([this]() { run(); });
}

void OutputFlusher::run()
{
    try {
        INFO_RELEASE("OutputFlusher::run starting" << taskName << " timeout " << timeout << "runnning " << running);
        while (running) {
            counter++;
            try {
                std::this_thread::sleep_for(std::chrono::milliseconds(timeout));
            } catch (const std::exception& e) {
                if (running) {
                    LOG_INFO_IMP("OutputFlusher::run" << e.what());
                    throw;
                }
            }

            if (writer) {
                if (counter % 100 == 0) {
                    LOG_TRACE("OutputFlusher::run " << counter << "writer flush");
                }
                writer->flushAll();
            }
        }
        INFO_RELEASE("OutputFlusher::run terminated " << taskName << " timeout " << timeout);
    } catch (const std::exception& t) {
        LOG_INFO_IMP("OutputFlusher error : " << t.what());
        notifyFlusherException(std::current_exception());
    }
}

void OutputFlusher::notifyFlusherException(const std::exception_ptr& e)
{
    try {
        if (e) {
            std::rethrow_exception(e);
        }
    } catch (const std::exception& ex) {
        std::cerr << "OutputFlusher exception: " << ex.what() << std::endl;
    }
}

} // namespace omnistream
