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

#ifndef STREAMING_FILE_WRITER_H
#define STREAMING_FILE_WRITER_H

#include <vector>
#include <string>
#include "AbstractStreamingWriter.h"

template <typename IN>
class StreamingFileWriter : public AbstractStreamingWriter<IN, int>
{
public:
    StreamingFileWriter(
        long bucketCheckInterval,
        BulkFormatBuilder<IN, std::string> *bucketsBuilder)
        : AbstractStreamingWriter<IN, int>(bucketCheckInterval, bucketsBuilder) {}

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
    {
        AbstractStreamingWriter<IN, int>::initializeState(initializer, keySerializer);
    }

    std::string getTypeName() override
    {
        return "StreamingFileWriter";
    }

    void processElement(StreamRecord *element) override {}

    void ProcessWatermark(Watermark *mark) override
    {
        AbstractStreamingWriter<IN, int>::processWatermark(mark);
    }

    void processBatch(StreamRecord *element) override
    {
        AbstractStreamingWriter<IN, int>::processBatch(element);
    }

protected:
    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override {}
};

#endif // STREAMING_FILE_WRITER_H