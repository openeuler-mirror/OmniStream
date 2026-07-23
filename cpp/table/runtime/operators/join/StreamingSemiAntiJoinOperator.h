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

// Description: streaming semi/anti join for SQL EXISTS/NOT EXISTS; emits left-only rows
// (semi: has right match; anti: no match, retracted on late match), reusing base of().

#pragma once

#include "AbstractStreamingJoinOperator.h"
#include "table/runtime/keyselector/KeySelector.h"
#include "table/data/util/VectorBatchUtil.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/src/vector/large_string_container.h"

template <typename K>
class StreamingSemiAntiJoinOperator : public AbstractStreamingJoinOperator<K> {
public:
    StreamingSemiAntiJoinOperator(const nlohmann::json& config, Output* output)
        : AbstractStreamingJoinOperator<K>(config, output)
    {
        this->output = output;
        LOG("<<<<<<SEMI/ANTI JOIN DESC:" << config.dump());
        if (config["joinType"] == "LeftSemiJoin") {
            isAntiJoin = false;
        } else if (config["joinType"] == "LeftAntiJoin") {
            isAntiJoin = true;
        } else {
            NOT_IMPL_EXCEPTION;
        }
    }

    virtual ~StreamingSemiAntiJoinOperator()
    {
        LOG(" >>> StreamingSemiAntiJoinOperator<K>::~StreamingSemiAntiJoinOperator");
        delete leftRecordStateView;
        leftRecordStateView = nullptr;
        delete rightRecordStateView;
        rightRecordStateView = nullptr;
    };

    void open() override;

    void processElement1(StreamRecord* element) override
    {
        NOT_IMPL_EXCEPTION;
    };

    void processElement2(StreamRecord* element) override
    {
        NOT_IMPL_EXCEPTION;
    };

    void processBatch1(StreamRecord* element) override
    {
        LOG("processBatch1(StreamRecord* element)");
        processBatchLeft(reinterpret_cast<omnistream::VectorBatch*>(element->getValue()));
        delete element;
    };

    void processBatch2(StreamRecord* element) override
    {
        LOG("processBatch2(StreamRecord* element)");
        processBatchRight(reinterpret_cast<omnistream::VectorBatch*>(element->getValue()));
        delete element;
    };

    void ProcessWatermark1(Watermark* watermark) override
    {
        LOG(">>>>>>>>>>");
        if (this->combinedWatermark->UpdateWatermark(0, watermark->getTimestamp())) {
            if (this->timeServiceManager != nullptr) {
                this->timeServiceManager->advanceWatermark(
                    new Watermark(this->combinedWatermark->GetCombinedWatermark()));
            }
            this->output->emitWatermark(new Watermark(this->combinedWatermark->GetCombinedWatermark()));
        }
    }
    void ProcessWatermark2(Watermark* watermark) override
    {
        LOG(">>>>>>>>>>");
        if (this->combinedWatermark->UpdateWatermark(1, watermark->getTimestamp())) {
            if (this->timeServiceManager != nullptr) {
                this->timeServiceManager->advanceWatermark(
                    new Watermark(this->combinedWatermark->GetCombinedWatermark()));
            }
            this->output->emitWatermark(new Watermark(this->combinedWatermark->GetCombinedWatermark()));
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

protected:
    // true for NOT EXISTS (anti), false for EXISTS (semi).
    bool isAntiJoin;
    JoinRecordStateView<K>* leftRecordStateView = nullptr;
    JoinRecordStateView<K>* rightRecordStateView = nullptr;
    int32_t maxParallelism;

    void processBatchLeft(omnistream::VectorBatch* input);
    void processBatchRight(omnistream::VectorBatch* input);

private:
    // Left-only output gathered from qualifying input rows (processBatch1, left arrives).
    omnistream::VectorBatch* buildOutputFromInput(omnistream::VectorBatch* input, const std::vector<bool>& qualify);
    // Left-only output fetched from left state by comboIDs (processBatch2, right arrives).
    omnistream::VectorBatch* buildOutputFromState(const std::vector<omnistream::ComboId>& comboIDs);

    template <typename T, typename S>
    omniruntime::vec::BaseVector* gatherInputColumn(
        omnistream::VectorBatch* input, int32_t icol, const std::vector<bool>& qualify, int32_t outRows);
    template <typename T, typename S>
    omniruntime::vec::BaseVector* gatherStateColumn(
        const std::vector<int32_t>& keyGroups, const std::vector<uint32_t>& sequenceNumbers,
        const std::vector<int32_t>& rowIds, int32_t icol, int32_t outRows);
    omniruntime::vec::BaseVector* gatherStateColumnVarchar(
        const std::vector<int32_t>& keyGroups, const std::vector<uint32_t>& sequenceNumbers,
        const std::vector<int32_t>& rowIds, int32_t icol, int32_t outRows);
};
