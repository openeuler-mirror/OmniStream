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

#ifndef FLINK_TNEL_INTERNALSERIALIZERS_H
#define FLINK_TNEL_INTERNALSERIALIZERS_H

#include "../types/logical/LogicalType.h"
#include "../../core/typeutils/TypeSerializer.h"

class InternalSerializers {
public:
    static TypeSerializer* create(LogicalType* type);

private:
    static TypeSerializer* createInternal(LogicalType* type);
};

#endif
