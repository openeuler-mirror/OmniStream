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

#ifndef CPP_STREAMEXPAND_H
#define CPP_STREAMEXPAND_H


#include <functional>
#include <iostream>
#include <nlohmann/json.hpp>
#include <variant>
#include <vector>

#include "Output.h"
#include "StreamCalcBatch.h"
#include "AbstractUdfStreamOperator.h"
#include "OneInputStreamOperator.h"

using ProjectFunc = int32_t (*)(const int64_t *, const uint8_t *, int32_t *, int64_t *, uint8_t *, int32_t *, int64_t);

class StreamExpand : public OneInputStreamOperator, public AbstractStreamOperator<int> {
public:
    explicit StreamExpand(const nlohmann::json &description, Output *output);

    ~StreamExpand() override;

    void processBatch(StreamRecord* record) override;

    void processElement(StreamRecord* record) override
    {
        NOT_IMPL_EXCEPTION
    };

    void open() override;

    void close() override;

    const char *getName() override;

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
    {
        LOG("StreamExpand initializeState()")
        // Do Nothing
    }
    void ProcessWatermark(Watermark *watermark) override
    {
        if (timeServiceManager != nullptr) {
            timeServiceManager->template advanceWatermark<int64_t>(watermark);
        }
        output->emitWatermark(watermark);
    }
    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override
    {
        output->emitWatermarkStatus(watermarkStatus);
    }

    std::string getTypeName() override
    {
        std::string typeName = "StreamExpand";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }

private:
    nlohmann::json description_;
};


#endif
