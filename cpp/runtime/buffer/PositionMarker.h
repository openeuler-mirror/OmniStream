/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef OMNISTREAM_POSITION_MARKER_H
#define OMNISTREAM_POSITION_MARKER_H

#include <string>
#include <sstream>
#include <cmath>
#include <atomic>
#include "core/include/common.h"
// check
namespace omnistream {

class PositionMarker {
public:
    static const int FINISHED_EMPTY = std::numeric_limits<int>::min();

    virtual int get() const = 0;

    static inline bool isFinished(int position)
    {
        return position < 0;
    }

    static inline int getAbsolute(int position)
    {
        if (unlikely(position == FINISHED_EMPTY)) {
            return 0;
        }
        return std::abs(position);
    }

    void addRef()
    {
        refCount_.fetch_add(1, std::memory_order_relaxed);
    }

    void release()
    {
        if (refCount_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete this;
        }
    }

    virtual ~PositionMarker() = default;

private:
    std::atomic<int> refCount_{1}; // 创建时引用计数为 1
};

} // namespace omnistream

#endif // OMNISTREAM_POSITION_MARKER_H
