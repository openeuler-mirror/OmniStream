//
// Created by root on 3/13/25.
//

#include "OutputFlusher.h"
#include "RecordWriterV2.h"
#include <chrono>
#include <stdexcept>
#include <iostream>

namespace omnistream {

    OutputFlusher::OutputFlusher(const std::string& name, long timeout, RecordWriterV2 * writer)
        : timeout(timeout), writer(writer),running(false), taskName(name) {
    }

    OutputFlusher::~OutputFlusher() {
        terminate();
        if (thread.joinable()) {
            thread.join();
        }
    }

    void OutputFlusher::terminate() {
        LOG_INFO_IMP("Terminating... " << taskName);
        running = false;
        if (thread.joinable()) {
            thread.join();
        }
    }

    void OutputFlusher::start() {
        if (thread.joinable()) {
            throw std::runtime_error("OutputFlusher already started.");
        }
        running = true;
        thread = std::thread([this]() { run(); });
    }



    void OutputFlusher::run() {
        try {
            LOG_INFO_IMP("OutputFlusher::run starting" << taskName  << " timeout " << timeout  << "runnning " << running );
            while (running) {
                counter++;
                try {
                    std::this_thread::sleep_for(std::chrono::milliseconds(timeout));
                } catch (const std::exception& e) {
                    if (running) {
                        LOG_INFO_IMP("OutputFlusher::run"<< e.what())
                        throw;
                    }
                }

                if (writer) {

                    if (counter %100 == 0)
                    {
                        LOG_TRACE("OutputFlusher::run "<< counter << "writer flush")
                    }

                    writer->flushAll();
                }
            }
            LOG_INFO_IMP("OutputFlusher::run terminated" << taskName  << " timeout " << timeout );
       } catch (const std::exception& t) {
           LOG_INFO_IMP("OutputFlusher error : " << t.what())
            notifyFlusherException(std::current_exception());
       }
    }

    void OutputFlusher::notifyFlusherException(const std::exception_ptr& e) {
        try {
            if (e) {
                std::rethrow_exception(e);
            }
        } catch (const std::exception& ex) {
            std::cerr << "OutputFlusher exception: " << ex.what() << std::endl;
        }
    }

} // namespace omnistream
