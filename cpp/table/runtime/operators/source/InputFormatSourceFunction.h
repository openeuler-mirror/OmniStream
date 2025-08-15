/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once

#include <iterator>
#include <memory>

#include "core/operators/StreamingRuntimeContext.h"
#include "core/typeinfo/TypeInfoFactory.h"
#include "core/typeinfo/TypeInformation.h"
#include "core/typeutils/TypeSerializer.h"
#include "functions/SourceContext.h"
#include "functions/SourceFunction.h"
#include "table/runtime/operators/source/InputSplit.h"
#include "table/runtime/operators/source/csv/CsvInputFormat.h"

namespace omnistream {

template <typename OUT>
class InputFormatSourceFunction : public SourceFunction<OUT>, public AbstractRichFunction {
public:
    InputFormatSourceFunction(omnistream::csv::CsvInputFormat<OUT>* format, InputSplit* inputSplit)
        :isRunning_(true), format_(format), inputSplit_(inputSplit)
    {
        std::ifstream inputStream_;
        inputStream_.open(inputSplit->getFilePath(), std::ios::in | std::ios::binary);
        if (!inputStream_.is_open()) {
            std::cerr << "Failed to open file: " << inputSplit->getFilePath() << std::endl;
            return;
        }
        int lineCount_ = 0;
        std::string line;
        while (std::getline(inputStream_, line)) {  // 逐行读取
            lineCount_++;  // 行数递增
        }
        lineCount = lineCount_;

        inputStream_.close();
    }

    void run(SourceContext* ctx)
    {
#ifdef TROUBLE_SHOOTING
        // in order to trigger back pressure
        for (int i = 0 ; i < 1000 ; i ++) {
#endif
        int parallelism = this->getRuntimeContext()->getNumberOfParallelSubtasks();
        int taskId = this->getRuntimeContext()->getIndexOfThisSubtask();

        int start = 0;
        int end = lineCount;
        int subMaxEvents = lineCount / parallelism;
        if (taskId == parallelism - 1) {
            start = subMaxEvents * (parallelism - 1);
            end = lineCount;
        } else {
            start = subMaxEvents * taskId;
            end = start + subMaxEvents;
        }
        int lineID = 0;

            format_->open(inputSplit_);

            while (isRunning_ && !format_->reachedEnd()) {
                OUT *nextElement = format_->nextRecord(lineID, start, end);

                if (nextElement != nullptr) {
                    ctx->collect(nextElement);
                } else {
                    break;
                }
                if (start >= end) {
                    break;
                }
            }

            format_->close();
#ifdef TROUBLE_SHOOTING
        }
#endif
    }

    void cancel()
    {
        isRunning_ = false;
    }

    void close()
    {
        format_->close();
    }

    omnistream::csv::CsvInputFormat<OUT>* getFormat()
    {
        return format_;
    }

    InputSplit* getInputSplit()
    {
        return inputSplit_;
    }

    bool isRunning()
    {
        return isRunning_;
    }

private:
    std::atomic<bool> isRunning_;
    omnistream::csv::CsvInputFormat<OUT>* format_;
    InputSplit* inputSplit_;
    int lineCount;
};

}  // namespace omnistream