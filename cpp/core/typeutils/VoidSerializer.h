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


#ifndef VOIDSERIALIZER_H
#define VOIDSERIALIZER_H

#include "TypeSerializerSingleton.h"

class VoidSerializer : public TypeSerializerSingleton {
public:
    const char* getName() const override
    {
        return "VoidSerializer";
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::VOID_NAMESPACE_BK;
    }

    void* deserialize(DataInputView& source) override
    {
        return nullptr;
    }
    void serialize(void* record, DataOutputSerializer& target) override {
    }
};

#endif  // VOIDSERIALIZER_H