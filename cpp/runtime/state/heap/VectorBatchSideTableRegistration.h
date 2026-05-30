/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#pragma once

#include "StateTable.h"
#include "runtime/state/VoidNamespace.h"
#include "table/data/vectorbatch/VectorBatch.h"

struct VectorBatchSideTableRegistration {
    StateTable<int, VoidNamespace, omnistream::VectorBatch *> *table = nullptr;
    int *nextBatchId = nullptr;
};
