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
#ifndef SLICINGWINDOWPROCESSOR_H
#define SLICINGWINDOWPROCESSOR_H

#include "table/data/vectorbatch/VectorBatch.h"
#include "core/typeutils/TypeSerializer.h"
#include "table/runtime/operators/window/SlicingWindowOperator.h"
#include "table/runtime/operators/InternalTimerService.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "streaming/api/operators/Output.h"
#include "functions/RuntimeContext.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"

template <typename W>
class SlicingWindowProcessor {
public:
    SlicingWindowProcessor() {};
    virtual void open(AbstractKeyedStateBackend<RowData*> *state, const nlohmann::json& config, StreamingRuntimeContext<RowData*> *runtimeCtx, InternalTimerServiceImpl<RowData*, int64_t>* internalTimerService)  = 0;
    virtual void initializeWatermark(int64_t watermark)  = 0;
    virtual bool processBatch(omnistream::VectorBatch* vectorbatch)  = 0;
    virtual void advanceProgress(StreamOperatorStateHandler<RowData*> *stateHandler, long progress) = 0 ;
    virtual void prepareCheckpoint()  = 0;
    virtual void fireWindow(W window)  = 0;
    virtual void clearWindow(W window)  = 0;
    virtual void close()  = 0;
    virtual TypeSerializer *createWindowSerializer() = 0;
    virtual Output* getOutput() = 0;
};

#endif
