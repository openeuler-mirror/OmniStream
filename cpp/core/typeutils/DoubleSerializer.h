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

#ifndef OMNISTREAM_DOUBLESERIALIZER_H
#define OMNISTREAM_DOUBLESERIALIZER_H
#include "TypeSerializerSingleton.h"
#include "basictypes/Double.h"

class DoubleSerializer : public TypeSerializerSingleton {
public:
    DoubleSerializer()
    {
        reuseBuffer = new Double();
    }
    ~DoubleSerializer() override = default;
    void* deserialize(DataInputView &source) override
    {
        NOT_IMPL_EXCEPTION
    }
    void serialize(void* record, DataOutputSerializer &target) override
    {
        NOT_IMPL_EXCEPTION
    }
    BackendDataType getBackendId() const override
    {
        return BackendDataType::OBJECT_BK;
    }
    static DoubleSerializer* INSTANCE;
    void deserialize(Object* buffer, DataInputView &source) override;
    void serialize(Object* buffer, DataOutputSerializer &target) override;

    Object* GetBuffer() override;
};

#endif // OMNISTREAM_DOUBLESERIALIZER_H
