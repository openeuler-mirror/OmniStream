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

#ifndef FLINK_TNEL_RECORDWRITERDELEGATE_H
#define FLINK_TNEL_RECORDWRITERDELEGATE_H

#include "RecordWriter.h"
namespace omnistream::datastream {
    class RecordWriterDelegate {
    public:
        virtual RecordWriter*  getRecordWriter(int outputIndex) = 0;

        virtual ~RecordWriterDelegate() = default;
    };
}
#endif
