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

#ifndef FLINK_TNEL_SINGLERECORDWRITER_H
#define FLINK_TNEL_SINGLERECORDWRITER_H

#include "../../../../../core/include/common.h"
#include "RecordWriterDelegate.h"

namespace omnistream::datastream {
    class SingleRecordWriter : public RecordWriterDelegate {
    public:
        explicit SingleRecordWriter(RecordWriter* recordWriter);

        RecordWriter* getRecordWriter(int outputIndex) override;

        ~SingleRecordWriter() override = default;

    private:
        RecordWriter* recordWriter_;
    };
}
#endif
