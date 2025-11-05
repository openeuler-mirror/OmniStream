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

#ifndef FLINK_TNEL_STREAMELEMENTSERIALIZER_H
#define FLINK_TNEL_STREAMELEMENTSERIALIZER_H

#include "core/typeutils/TypeSerializer.h"
#include "core/include/common.h"
#include "StreamElement.h"
#include "StreamRecord.h"
#include "streaming/api/watermark/Watermark.h"

namespace omnistream::datastream {
    class StreamElementSerializer : public TypeSerializer {
    public:
        explicit StreamElementSerializer(TypeSerializer *typeSerializer);

        ~StreamElementSerializer() override
        {
            delete reUsableRecord_;
            delete reUsableWatermark_;
            delete typeSerializer_;
        }

        void *deserialize(DataInputView& source) override;
        void serialize(void * record, DataOutputSerializer& target) override {}

        void serialize(Object *record, DataOutputSerializer &target) override;

        const char *getName() const override;

        BackendDataType getBackendId() const override
        {
            return BackendDataType::BIGINT_BK;
        };

    private:
        TypeSerializer *typeSerializer_;
        StreamRecord *reUsableRecord_{};
        Watermark *reUsableWatermark_{};
    };
}
#endif
