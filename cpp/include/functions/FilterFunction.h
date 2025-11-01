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

#ifndef FLINK_TNEL_FILTERFUNCTION_H
#define FLINK_TNEL_FILTERFUNCTION_H

#include <memory>
#include <vector>
#include "basictypes/Object.h"
#include "common.h"
/**
 * T: such as Object, VectorBatch
 * */
template<typename T>
class FilterFunction {
public:
    virtual ~FilterFunction() = default;

    virtual bool filter(Object* input)
    {
        NOT_IMPL_EXCEPTION
    };

    virtual std::vector<int> filterBatch(void* input)
    {
        NOT_IMPL_EXCEPTION
    };
};

template<typename T>
using FilterFunctionUnique = std::unique_ptr<FilterFunction<T>>;
#endif // FLINK_TNEL_FILTERFUNCTION_H
