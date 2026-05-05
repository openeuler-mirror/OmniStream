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

#ifndef OMNISTREAM_REGISTEREDBROADCASTSTATEBACKENDMETAINFO_H
#define OMNISTREAM_REGISTEREDBROADCASTSTATEBACKENDMETAINFO_H

#include <string>
#include <unordered_map>
#include <memory>

#include "core/typeutils/TypeSerializer.h"
#include "core/typeutils/TypeSerializerSnapshot.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"

#include "OperatorStateHandle.h"
#include "RegisteredStateMetaInfoBase.h"

class RegisteredBroadcastStateBackendMetaInfo : public RegisteredStateMetaInfoBase {
public:
    RegisteredBroadcastStateBackendMetaInfo(
        const std::string& name_,
        OperatorStateHandle::Mode assignmentMode_,
        TypeSerializer* keySerializer_,
        TypeSerializer* valueSerializer_);

    explicit RegisteredBroadcastStateBackendMetaInfo(const StateMetaInfoSnapshot& snapshot_);

    std::shared_ptr<StateMetaInfoSnapshot> snapshot() override { return computeSnapshot(); }

    OperatorStateHandle::Mode getAssignmentMode() { return assignmentMode; }

    void updateAssignmentMode(OperatorStateHandle::Mode assignmentMode_) { assignmentMode = assignmentMode_; }

    TypeSerializer* getKeySerializer() { return keySerializer; }

    void updateKeySerializer(TypeSerializer* keySerializer_) { keySerializer = keySerializer_; }

    TypeSerializer* getValueSerializer() { return valueSerializer; }

    void updateValueSerializer(TypeSerializer* valueSerializer_) { valueSerializer = valueSerializer_; }

private:
    OperatorStateHandle::Mode assignmentMode;
    TypeSerializer* keySerializer;
    TypeSerializer* valueSerializer;

    std::shared_ptr<StateMetaInfoSnapshot> computeSnapshot();
};

#endif //OMNISTREAM_REGISTEREDBROADCASTSTATEBACKENDMETAINFO_H
