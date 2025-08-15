#pragma once

#include <memory>
#include <thread>
#include <exception>
#include <string>
#include <vector>


namespace omnistream
{
    class RecordWriterV2;

    class OutputFlusher {
    public:
        OutputFlusher(const std::string& name, long timeout, RecordWriterV2 * writer);
        ~OutputFlusher();

        void terminate();
        void run();
        void start();

    private:
        long timeout;
       RecordWriterV2 *  writer;
        std::thread thread;
        bool running;

        std::string taskName;

        // trouble shooting
        long counter = 0;


        void notifyFlusherException(const std::exception_ptr& e);
    };
}
