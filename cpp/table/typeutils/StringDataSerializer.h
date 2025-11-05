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
#ifndef FLINK_TNEL_STRINGDATASERIALIZER_H
#define FLINK_TNEL_STRINGDATASERIALIZER_H

#include <memory>
#include "../../core/typeutils/TypeSerializerSingleton.h"
#include "../data/StringData.h"
#include "OmniOperatorJIT/core/src/type/data_type.h"

using namespace omniruntime::type;

class StringDataSerializer : public TypeSerializerSingleton {
public:
    StringDataSerializer() {};
    void *deserialize(DataInputView &source) override;

    void serialize(void *record, DataOutputSerializer &target) override;

    static StringDataSerializer* INSTANCE;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::VARCHAR_BK;
    }
};

#endif
