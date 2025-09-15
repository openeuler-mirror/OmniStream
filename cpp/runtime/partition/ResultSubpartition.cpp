/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "ResultSubpartition.h"

omnistream::ResultSubpartition::ResultSubpartition(int index, std::shared_ptr<ResultPartition> parent):
parent(parent)
{
    subpartitionInfo = ResultSubpartitionInfoPOD(parent->getPartitionIndex(), index);
}

omnistream::ResultSubpartition::~ResultSubpartition() {}