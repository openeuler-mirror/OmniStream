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

#pragma once

#include <string>
#include "RegisteredStateMetaInfoBase.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/StateDescriptor.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "typeutils/TypeSerializerSchemaCompatibility.h"

class RegisteredPriorityQueueStateBackendMetaInfo : public RegisteredStateMetaInfoBase {
public:
    explicit RegisteredPriorityQueueStateBackendMetaInfo(const StateMetaInfoSnapshot &snapshot);

    RegisteredPriorityQueueStateBackendMetaInfo( const std::string& name, TypeSerializer* elementSerializer)
        : RegisteredStateMetaInfoBase(name),
          elementSerializer(elementSerializer),
          previousElementSerializer(elementSerializer) {}

    std::shared_ptr<StateMetaInfoSnapshot> snapshot() override { return computeSnapshot(); }

    TypeSerializer* getElementSerializer() { return elementSerializer; }

    TypeSerializer* getPreviousElementSerializer();

    std::shared_ptr<RegisteredPriorityQueueStateBackendMetaInfo> withSerializerUpgradesAllowed() {
        auto copy = std::make_shared<RegisteredPriorityQueueStateBackendMetaInfo>(*this);
        copy->serializerUpdatesAllowed = true;
        return copy;
    }

    TypeSerializerSchemaCompatibility updateElementSerializer(TypeSerializer* serializer);

private:
    TypeSerializer* elementSerializer;
    TypeSerializer* previousElementSerializer = nullptr;
    bool serializerUpdatesAllowed = false;

    std::shared_ptr<StateMetaInfoSnapshot> computeSnapshot();
};
