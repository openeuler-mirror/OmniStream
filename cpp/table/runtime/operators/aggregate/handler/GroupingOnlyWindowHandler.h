/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#pragma once

#include "table/data/GenericRowData.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"

// This is an only window handler (without agg) that emits only the session‐window boundaries.
// It returns a 4‐field Binary Row: [ windowStart, windowEnd, windowEnd‐1, null ]

template<typename N>
class GroupingOnlyWindowHandler : public NamespaceAggsHandleFunction<N> {
public:
GroupingOnlyWindowHandler() = default;

    void open(StateDataViewStore *store) override
    {
        this->store = store;
    }

    void accumulate(RowData *accInput) override {}

    void retract(RowData *retractInput) override {}

    void merge(N ns, RowData *otherAcc) override {}

    void setAccumulators(N ns, RowData *acc) override {}

    RowData *getAccumulators() override
    {
        LOG("GroupingOnlyWindowHandler::getAccumulators");
        BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(1);
        currentAcc->setLong(0, (long) 0);
        return currentAcc;
    }

    RowData *createAccumulators(int x) override
    {
        LOG("GroupingOnlyWindowHandler::createAccumulators");
        BinaryRowData *currentAcc = BinaryRowData::createBinaryRowDataWithMem(1);
        currentAcc->setLong(0, 0L);
        return currentAcc;
    }

    RowData *getValue(TimeWindow ns)
    {
        BinaryRowData *aggValue = BinaryRowData::createBinaryRowDataWithMem(4);

        constexpr int START_TS = 0;
        constexpr int END_TS = 1;
        constexpr int MAX_TS = 2;
        constexpr int PLACEHOLDER = 3;

        aggValue->setLong(START_TS, ns.getStart());
        aggValue->setLong(END_TS, ns.getEnd());
        aggValue->setLong(MAX_TS, ns.getEnd() - 1);
        aggValue->setLong(PLACEHOLDER, -1);

        return aggValue;
    }

    void Cleanup(TimeWindow ns) override {}

    void close() override {}

private:
    StateDataViewStore *store{};
};
