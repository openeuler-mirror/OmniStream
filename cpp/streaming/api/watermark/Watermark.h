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

#ifndef FLINK_TNEL_WATERMARK_H
#define FLINK_TNEL_WATERMARK_H

#include <cstdint>
#include <limits.h>
#include "streaming/runtime/streamrecord/StreamElement.h"

class Watermark : public StreamElement {
public:
    static const Watermark MAX_WATERMARK;

    explicit Watermark(int64_t timestamp);

    int64_t getTimestamp() const;

    void setTimestamp(int64_t timestamp);

private:
    int64_t  timestamp_;
};


#endif // FLINK_TNEL_WATERMARK_H
