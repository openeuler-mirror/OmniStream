/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef PARTITION_COMMITTER_H
#define PARTITION_COMMITTER_H

class PartitionCommitter : public OneInputStreamOperator {
public:
    PartitionCommitter() {}

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override {}

    void processElement(StreamRecord *element) override {}

    void ProcessWatermark(Watermark *mark) override {}

    void processBatch(StreamRecord *element) override {}

    std::string getTypeName() override
    {
        return "PartitionCommitter";
    }

protected:
    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override {}
};

#endif // PARTITION_COMMITTER_H