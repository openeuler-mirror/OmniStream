/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_WINDOWLISTSTATE_H
#define FLINK_TNEL_WINDOWLISTSTATE_H

#include <array>
#include <tuple>
#include "runtime/state/heap/HeapListState.h"
#include "WindowState.h"

template <typename KeyType, typename W, typename ValType>
class WindowListState : public WindowState<W> {
public:
    WindowListState(InternalListState<KeyType, W, ValType> *windowState) : windowState(windowState) {};
    ~WindowListState() override;
    void clear(W window);
    std::vector<ValType> *get(W window);
    void add(W window, ValType value);
    void addVectorBatch(omnistream::VectorBatch *batch);
    const std::vector<omnistream::VectorBatch *> &getVectorBatches();
    int getCurrentBatchId();
    void clearVectors(int64_t currentTimestamp) { windowState->clearVectors(currentTimestamp); };
    omnistream::VectorBatch *getVectorBatch(int batchId);
private:
    InternalListState<KeyType, W, ValType> *windowState;
};

template <typename KeyType, typename W, typename ValType>
void WindowListState<KeyType, W, ValType>::clear(W window)
{
    windowState->setCurrentNamespace(window);
    windowState->clear();
}

template <typename KeyType, typename W, typename ValType>
WindowListState<KeyType, W, ValType>::~WindowListState()
{
    delete windowState;
}

template <typename KeyType, typename W, typename ValType>
std::vector<ValType>* WindowListState<KeyType, W, ValType>::get(W window)
{
    windowState->setCurrentNamespace(window);
    return windowState->get();
}

template <typename KeyType, typename W, typename ValType>
void WindowListState<KeyType, W, ValType>::add(W window, ValType value)
{
    windowState->setCurrentNamespace(window);
    windowState->add(value);
}

template <typename KeyType, typename W, typename ValType>
void WindowListState<KeyType, W, ValType>::addVectorBatch(omnistream::VectorBatch *batch)
{
    windowState->addVectorBatch(batch);
}

template <typename KeyType, typename W, typename ValType>
const std::vector<omnistream::VectorBatch *> &WindowListState<KeyType, W, ValType>::getVectorBatches()
{
    return windowState->getVectorBatches();
}

template <typename KeyType, typename W, typename ValType>
omnistream::VectorBatch *WindowListState<KeyType, W, ValType>::getVectorBatch(int batchId)
{
    return windowState->getVectorBatch(batchId);
}

template <typename KeyType, typename W, typename ValType>
int WindowListState<KeyType, W, ValType>::getCurrentBatchId()
{
    return windowState->getVectorBatchesSize();
}


#endif // FLINK_TNEL_WINDOWLISTSTATE_H
