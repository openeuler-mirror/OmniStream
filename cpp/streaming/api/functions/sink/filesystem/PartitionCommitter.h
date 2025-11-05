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