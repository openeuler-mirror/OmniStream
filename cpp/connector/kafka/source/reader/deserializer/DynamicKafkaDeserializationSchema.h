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

#ifndef OMNISTREAM_DYNAMICKAFKADESERIALIZATIONSCHEMA_H
#define OMNISTREAM_DYNAMICKAFKADESERIALIZATIONSCHEMA_H

#include "core/api/common/serialization/DeserializationSchema.h"

class DynamicKafkaDeserializationSchema : public DeserializationSchema {
public:
    explicit DynamicKafkaDeserializationSchema(DeserializationSchema* valueDeserialization)
        : valueDeserialization(valueDeserialization)
    {
    }

    void Open() override
    {
    }

    bool isEndOfStream(const void* nextElement) override
    {
        return false;
    }

private:
    DeserializationSchema* valueDeserialization;
};


#endif // OMNISTREAM_DYNAMICKAFKADESERIALIZATIONSCHEMA_H
