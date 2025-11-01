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
#ifndef FLINK_TNEL_RICH_FUNCTION_H
#define FLINK_TNEL_RICH_FUNCTION_H

#include "RuntimeContext.h"
#include "IterationRuntimeContext.h"
#include "OpenContext.h"
#include "Configuration.h"

class RichFunction {
public:
    virtual ~RichFunction();

    virtual void open(const Configuration &parameters);

    virtual void open(Configuration* parameters);

    virtual void open(const OpenContext *openContext);

    virtual void close() = 0;

    virtual RuntimeContext *getRuntimeContext() = 0;

    virtual IterationRuntimeContext *getIterationRuntimeContext() = 0;

    virtual void setRuntimeContext(RuntimeContext *context) = 0;
};

#endif // FLINK_TNEL_RICH_FUNCTION_H
