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

#ifndef FLINK_TNEL_RECORDEMITTER_H
#define FLINK_TNEL_RECORDEMITTER_H

#include <memory>
#include "core/api/connector/source/SourceOutput.h"


template <typename E, typename SplitStateT>
class RecordEmitter {
public:
    virtual void emitRecord(E* consumerRecord, SourceOutput* output,
        SplitStateT* splitState) = 0;
    virtual void emitBatchRecord(const std::vector<E*>& messageVec, SourceOutput* output,
        SplitStateT* splitState) = 0;
    // 虚析构函数，确保正确释放派生类对象
    virtual ~RecordEmitter() = default;
};

#endif // FLINK_TNEL_RECORDEMITTER_H
