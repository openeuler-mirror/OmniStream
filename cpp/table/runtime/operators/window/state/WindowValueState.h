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
#ifndef WINDOWVALUESTATE_H
#define WINDOWVALUESTATE_H

#include "WindowState.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "state/internal/InternalValueState.h"

template<typename KeyType, typename W, typename ValType>
class WindowValueState : public WindowState<W> {
public:
    WindowValueState(InternalValueState<KeyType, W, ValType> *windowState) : windowState(windowState) {
        isFalconEnabled_ = dynamic_cast<RocksdbValueState<KeyType, W, ValType>*>(windowState) != nullptr &&
                          dynamic_cast<RocksdbValueState<KeyType, W, ValType>*>(windowState)->isFalconEnabled();
    }

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

    bool isFalconEnabled() const {
        return isFalconEnabled_;
    }
private:
    InternalValueState<KeyType, W, ValType>* windowState;
    bool isFalconEnabled_ = false;
};

#endif
