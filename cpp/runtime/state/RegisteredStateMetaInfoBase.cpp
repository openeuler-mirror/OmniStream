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

#include "RegisteredStateMetaInfoBase.h"

#include "RegisteredOperatorStateBackendMetaInfo.h"
#include "runtime/state/RegisteredKeyValueStateBackendMetaInfo.h"
#include "runtime/state/RegisteredOperatorStateBackendMetaInfo.h"
#include "runtime/state/RegisteredBroadcastStateBackendMetaInfo.h"
#include "runtime/state/RegisteredPriorityQueueStateBackendMetaInfo.h"

std::unique_ptr<RegisteredStateMetaInfoBase> RegisteredStateMetaInfoBase::fromMetaInfoSnapshot(const
    StateMetaInfoSnapshot &snapshot)
{
    const StateMetaInfoSnapshot::BackendStateType backendStateType =
            snapshot.getBackendStateType();
    switch (backendStateType) {
        case StateMetaInfoSnapshot::BackendStateType::KEY_VALUE:
            return std::make_unique<RegisteredKeyValueStateBackendMetaInfo>(snapshot);
        case StateMetaInfoSnapshot::BackendStateType::OPERATOR:
            return std::make_unique<RegisteredOperatorStateBackendMetaInfo>(snapshot);
        case StateMetaInfoSnapshot::BackendStateType::BROADCAST:
            return std::make_unique<RegisteredBroadcastStateBackendMetaInfo>(snapshot);
        case StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE:
            return std::make_unique<RegisteredPriorityQueueStateBackendMetaInfo>(snapshot);
        default:
            LOG("Unsupport backend state type: + std::to_string(static_cast<int>(backendStateType))")
            return nullptr;
            // tO-DO the OPERATOR, BROADCAST, PRIORITY_QUEUE is not used currently!
    }
}
