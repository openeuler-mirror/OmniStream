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

#pragma once

#include "AbstractStreamingJoinOperator.h"
#include "state/JoinRecordStateView.h"
#include "state/OuterJoinRecordStateView.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "OmniOperatorJIT/core/src/vector/large_string_container.h"

#include <memory>
#include <stdexcept>
#include <vector>

namespace omnistream {
template <typename K>
class StreamingSemiAntiJoinOperator : public AbstractStreamingJoinOperator<K> {
public:
    StreamingSemiAntiJoinOperator(const nlohmann::json& config, Output* output)
        : AbstractStreamingJoinOperator<K>(config, output)
    {
        this->output = output;
        LOG("<<<<<<SEMI/ANTI JOIN DESC:" << config.dump());
        if (config["joinType"] == "LeftSemiJoin") {
            isAntiJoin_ = false;
        } else if (config["joinType"] == "LeftAntiJoin") {
            isAntiJoin_ = true;
        } else {
            NOT_IMPL_EXCEPTION;
        }
    }

    ~StreamingSemiAntiJoinOperator() override
    {
        LOG(" >>> StreamingSemiAntiJoinOperator<K>::~StreamingSemiAntiJoinOperator");
    }

    void open() override;

    void processElement1(StreamRecord* element) override
    {
        NOT_IMPL_EXCEPTION;
    }

    void processElement2(StreamRecord* element) override
    {
        NOT_IMPL_EXCEPTION;
    }

    void processBatch1(StreamRecord* element) override
    {
        LOG("processBatch1(StreamRecord* element)");
        auto input = reinterpret_cast<omnistream::VectorBatch*>(element->getValue());
        processBatch(input, true);
        delete element;
    }

    void processBatch2(StreamRecord* element) override
    {
        LOG("processBatch2(StreamRecord* element)");
        auto input = reinterpret_cast<omnistream::VectorBatch*>(element->getValue());
        processBatch(input, false);
        delete element;
    }

    void ProcessWatermark1(Watermark* watermark) override
    {
        if (this->combinedWatermark->UpdateWatermark(0, watermark->getTimestamp())) {
            auto newWatermark = Watermark(this->combinedWatermark->GetCombinedWatermark());
            if (this->timeServiceManager != nullptr) {
                this->timeServiceManager->advanceWatermark(&newWatermark);
            }
            this->output->emitWatermark(&newWatermark);
        }
    }

    void ProcessWatermark2(Watermark* watermark) override
    {
        if (this->combinedWatermark->UpdateWatermark(1, watermark->getTimestamp())) {
            auto newWatermark = Watermark(this->combinedWatermark->GetCombinedWatermark());
            if (this->timeServiceManager != nullptr) {
                this->timeServiceManager->advanceWatermark(&newWatermark);
            }
            this->output->emitWatermark(&newWatermark);
        }
    }

    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        return this->metrics;
    }

    std::string getTypeName() override
    {
        return this->opName;
    }

private:
    bool isAntiJoin_ = false;
    std::unique_ptr<OuterJoinRecordStateView> leftRecordStateView_;
    std::unique_ptr<JoinRecordStateView> rightRecordStateView_;

    std::vector<std::shared_ptr<RowData>> outputRows_;
    std::vector<RowKind> outputKinds_;
    std::vector<int64_t> outputTimestamps_;
    int64_t currentInputRowTimestamp_ = 0;

    void processBatch(omnistream::VectorBatch* input, bool isLeft);
    void processElementLeft(const std::shared_ptr<RowData>& input);
    void processElementRight(const std::shared_ptr<RowData>& input);
    omnistream::VectorBatch* buildOutputVectorBatch();

    void collectLeft(const std::shared_ptr<RowData>& row, RowKind kind)
    {
        outputRows_.push_back(row);
        outputKinds_.push_back(kind);
        outputTimestamps_.push_back(currentInputRowTimestamp_);
    }
};

extern template class StreamingSemiAntiJoinOperator<RowData*>;
extern template class StreamingSemiAntiJoinOperator<long>;
} // namespace omnistream
