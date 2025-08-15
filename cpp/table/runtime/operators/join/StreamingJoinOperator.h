/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by xichen on 9/27/24.
//

#ifndef FLINK_TNEL_STREAMINGJOINOPERATOR_H
#define FLINK_TNEL_STREAMINGJOINOPERATOR_H
#include "AbstractStreamingJoinOperator.h"
#include "table/data/utils/JoinedRowData.h"
#include "table/KeySelector.h"
#include "table/data/GenericRowData.h"
#include "table/data/util/RowDataUtil.h"
#include "table/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/src/vector/large_string_container.h"
#include <arm_sve.h>

template <typename K>
class StreamingJoinOperator : public AbstractStreamingJoinOperator<K> {
public:
    StreamingJoinOperator(const nlohmann::json&  config, Output * output)
            :AbstractStreamingJoinOperator<K> (config, output)
    {
        this->output = output;
        LOG("<<<<<<JOIN DESC:" << config.dump())
        filterNullKeys = config["filterNulls"].get<std::vector<bool>>();
        if (config["joinType"] == "InnerJoin") {
            leftIsOuter = false;
            rightIsOuter = false;
        } else if (config["joinType"] == "LeftOuterJoin") {
            leftIsOuter = true;
            rightIsOuter = false;
        } else {
            NOT_IMPL_EXCEPTION
        }
    }

    virtual ~StreamingJoinOperator()
    {
        LOG(" >>> StreamingJoinOperator<K>::~StreamingJoinOperator");
    };

    void open() override;

    void processElement1(StreamRecord* element) override
    {
        NOT_IMPL_EXCEPTION
    };

    void processElement2(StreamRecord* element) override
    {
        NOT_IMPL_EXCEPTION
    };

    void processBatch1(StreamRecord* element) override
    {
        LOG("processBatch1(StreamRecord* element)")
        processBatch(reinterpret_cast<omnistream::VectorBatch*>(element->getValue()), leftRecordStateView,
                     rightRecordStateView, true, false);
        delete element;
    };

    void processBatch2(StreamRecord* element) override
    {
        LOG("processBatch2(StreamRecord* element)")
        processBatch(reinterpret_cast<omnistream::VectorBatch*>(element->getValue()), rightRecordStateView,
                     leftRecordStateView, false, false);
        delete element;
    };

    void ProcessWatermark1(Watermark* watermark) override
    {
        AbstractStreamOperator<K>::ProcessWatermark1(watermark);
    }
    void ProcessWatermark2(Watermark* watermark) override
    {
        AbstractStreamOperator<K>::ProcessWatermark2(watermark);
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
    bool leftIsOuter;
    bool rightIsOuter;
    std::vector<bool> filterNullKeys;
    JoinRecordStateView<K>* leftRecordStateView;
    JoinRecordStateView<K>* rightRecordStateView;
    void processElement(
            RowData *input,
            JoinRecordStateView<K> *inputSideStateView,
            JoinRecordStateView<K> *otherSideStateView,
            bool inputIsLeft,
            bool isSuppress)
    {
        NOT_IMPL_EXCEPTION
    };

    void processBatch(
            omnistream::VectorBatch *input,
            JoinRecordStateView<K> *inputSideStateView,
            JoinRecordStateView<K> *otherSideStateView,
            bool inputIsLeft,
            bool isSuppress);

private:

    omnistream::VectorBatch* buildOutput(omnistream::VectorBatch *input, bool inputIsLeft,
                                         bool inputIsOuter, bool otherIsOuter,
                                         JoinRecordStateView<K> *otherSideStateView)
    {
        // Total number of output rows are:
        // (1) one row for each unique match
        // (2) one row for each record that didnt find a match (if input is outer)
        // (3) one row for each record on the other side that just found its first match and its null-padded entry needs to be deleted
        this->matchedCountTot = 0;
        for (int count : this->matchedCount) {
            if (count != 0) { // (1)
                this->matchedCountTot += count;
            } else if (inputIsOuter) { // (2)
                this->matchedCountTot++;
            }
        }
        this->matchedCountTot += this->deleteRecords.size(); // (3)
        if (this->matchedCountTot == 0) {
            return nullptr;
        }
        omnistream::VectorBatch* outputVB = new omnistream::VectorBatch(this->matchedCountTot);
        outputVB->ResizeVectorCount(this->leftInputTypes.size() + this->rightInputTypes.size());
        // Build the columns that comes from inputSide
        setOutPutValueInput(input, inputIsLeft, inputIsOuter, otherSideStateView, outputVB);
        // Build the columns that comes from the otherSide
        setOutPutValueOther(input, inputIsLeft, inputIsOuter, otherSideStateView, outputVB);
        // set the RowKind and timestamp. Only when both sides uses inner join, it uses the input's RowKind and Timestamp
        setOutPutMetaData(input, inputIsOuter, otherIsOuter, outputVB);
        return outputVB;
    };

    omnistream::VectorBatch* buildOutputInner(omnistream::VectorBatch *input, bool inputIsLeft, JoinRecordStateView<K> *otherSideStateView);

    template<typename T, typename S>
    omniruntime::vec::BaseVector* buildInputSideColumn(omnistream::VectorBatch *input, int32_t icol, bool inputIsOuter);

    template <typename T, typename S>
    omniruntime::vec::BaseVector* buildOtherSideColumn(omnistream::VectorBatch *input, JoinRecordStateView<K> *otherSideStateView, int32_t icol, bool inputIsOuter);

    omniruntime::vec::BaseVector* buildOtherSideColumnVarchar(omnistream::VectorBatch *input, JoinRecordStateView<K> *otherSideStateView, int32_t icol, bool inputIsOuter);

    void setOutPutValueInput(omnistream::VectorBatch *input, bool inputIsLeft, bool inputIsOuter, JoinRecordStateView<K> *otherSideStateView,
                             omnistream::VectorBatch* outputVB);

    void setOutPutValueOther(omnistream::VectorBatch *input, bool inputIsLeft, bool inputIsOuter, JoinRecordStateView<K> *otherSideStateView,
                             omnistream::VectorBatch* outputVB);

    void setOutPutMetaData(omnistream::VectorBatch *input, bool inputIsOuter, bool otherIsOuter,
                           omnistream::VectorBatch* outputVB);

    RowKind getOutputVBRowKind(omnistream::VectorBatch *input, bool inputIsOuter, bool otherIsOuter, int index);

    void setRowKind_sve(int i, int size, uint8_t* dst, int8_t* condition);

    void setTimestamp_raw(int start, int size, const int64_t* src, int64_t* dst, int rowIndex);

    void AssembleFisrtTime(omnistream::VectorBatch* input, omnistream::VectorBatch* outputVB, bool inputIsLeft);

    void AssembleSecondTime(omnistream::VectorBatch* input, omnistream::VectorBatch* outputVB,
                            JoinRecordStateView<K> *otherSideStateView, bool inputIsLeft);
    void DealOneBatchInColumnVarchar(long id, int32_t icol, int& rowIndex, JoinRecordStateView<K> *otherSideStateView,
                                     omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*& outputCol);
    template <typename T, typename S>
    void DealOneBatchInColumn(long id, int32_t icol, int& rowIndex, int& curbatchId,
                              JoinRecordStateView<K> *otherSideStateView, omniruntime::vec::Vector<S>*& inputCol,
                              omniruntime::vec::Vector<T>*& outputCol);
};
#endif