/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef WINDOWVALUESTATE_H
#define WINDOWVALUESTATE_H

#include "WindowState.h"
#include "core/api/ValueState.h"
#include "table/data/RowData.h"
#include "runtime/state/heap/HeapValueState.h"

template<typename KeyType, typename W, typename ValType>
class WindowValueState : public WindowState<W> {
public:
    WindowValueState(HeapValueState<KeyType, W, ValType> *windowState) : windowState(windowState) {};

    void clear(W window) override
    {
        windowState->setCurrentNamespace(window);
        windowState->clear();
    };

    ValType value(W window)
    {
        windowState->setCurrentNamespace(window);
        return windowState->value();
    };

    void update(W window, ValType value)
    {
        windowState->setCurrentNamespace(window);
        windowState->update(value);
    };

    ~WindowValueState() override
    {
        delete windowState;
    };

    void addVectorBatch(omnistream::VectorBatch *batch)
    {
        windowState->addVectorBatch(batch);
    };

    const std::vector<omnistream::VectorBatch *> &getVectorBatches()
    {
        return windowState->getVectorBatches();
    };

    int getCurrentBatchId()
    {
        return windowState->getVectorBatchesSize();
    };
private:
    HeapValueState<KeyType, W, ValType> *windowState;
};

#endif
