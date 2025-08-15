/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/19/25.
//

#pragma once

#include "table/data/GenericRowData.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"

template<typename N>
class GroupingWindowAggsCountHandler : public NamespaceAggsHandleFunction<N> {
public:
    GroupingWindowAggsCountHandler() = default;

    void open(StateDataViewStore *store) override
    {
        this->store = store;
    }

    void accumulate(RowData *accInput) override
    {
        if (!isNulll) {
            count++;
        }
    }

    void retract(RowData *retractInput) override
    {
        throw std::runtime_error("This function not require retract method, but the retract method is called.");
    }

    void merge(N ns, RowData *otherAcc) override
    {
        isNulll = isNulll || otherAcc->isNullAt(0);
        if (!isNulll) {
            count = count + *otherAcc->getLong(0);
        } else {
            count = -1;
        }
    }

    void setAccumulators(N ns, RowData *acc) override
    {
        if (!acc->isNullAt(0)) {
            count = *acc->getLong(0);
        } else {
            count = -1;
        }
    }

    RowData *getAccumulators() override
    {
        acc10 = BinaryRowData::createBinaryRowDataWithMem(1);
        if (isNulll) {
            acc10->setLong(0, -1);
        } else {
            acc10->setLong(0, count);
        }
        return acc10;
    }

    RowData *createAccumulators(int x) override
    {
        acc9 = BinaryRowData::createBinaryRowDataWithMem(1);
        acc9->setLong(0, 0L);
        return acc9;
    }

    RowData *getValue(TimeWindow ns)
    {
        aggValue = BinaryRowData::createBinaryRowDataWithMem(5);
        if (isNulll) {
            aggValue->setLong(0, -1);
        } else {
            aggValue->setLong(0, count);
        }

        aggValue->setLong(1, ns.getStart());
        aggValue->setLong(2, ns.getEnd());
        aggValue->setLong(3, ns.getEnd() - 1);
        aggValue->setLong(4, -1);

        return aggValue;
    }

    void Cleanup(TimeWindow ns) override {}

    void close() override {}

private:
    long count{};
    bool isNulll{};
    BinaryRowData *acc9 = BinaryRowData::createBinaryRowDataWithMem(1);
    BinaryRowData *acc10 = BinaryRowData::createBinaryRowDataWithMem(1);
    BinaryRowData *aggValue = BinaryRowData::createBinaryRowDataWithMem(5);
    StateDataViewStore *store{};
    TimeWindow ns;
};
