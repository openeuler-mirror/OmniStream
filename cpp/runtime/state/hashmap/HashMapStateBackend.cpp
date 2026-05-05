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
#include "HashMapStateBackend.h"

template <typename K>
AbstractKeyedStateBackend<K> *HashMapStateBackend::createKeyedStateBackend(
    omnistream::EnvironmentV2 *env,
    std ::set<KeyedStateHandle> stateHandles,
    KeyGroupRange *keyGroupRange,
    TypeSerializer *keySerializer,
    int numberOfKeyGroups)
{
    // TTODO
    // Get recovery config
    restoreState();
    HeapKeyedStateBackendBuilder<K> builder = HeapKeyedStateBackendBuilder<K>(keySerializer, numberOfKeyGroups, keyGroupRange);
    return builder.build();
}

OperatorStateBackend* HashMapStateBackend::createOperatorStateBackend(
    omnistream::EnvironmentV2* env_,
    std::string operatorIdentifier_,
    std::set<std::shared_ptr<OperatorStateHandle>> stateHandles_) {
    INFO_RELEASE("h30082497 HashMapStateBackend:createOperatorStateBackend 1");
    std::vector<std::shared_ptr<OperatorStateHandle>> stateVector(stateHandles_.begin(), stateHandles_.end());
    INFO_RELEASE("h30082497 HashMapStateBackend:createOperatorStateBackend 2");
    auto bridge = env_->getTaskStateManager()->getTaskStateManagerBridge();
    INFO_RELEASE("h30082497 HashMapStateBackend:createOperatorStateBackend 3");
    auto omniTaskBridge = env_->getTaskStateManager()->getOmniTaskBridge();
    INFO_RELEASE("h30082497 HashMapStateBackend:createOperatorStateBackend 4");

    const bool asynchronousSnapshots = true;
    DefaultOperatorStateBackendBuilder builder(
        asynchronousSnapshots,
        operatorIdentifier_,
        stateVector,
        bridge,
        omniTaskBridge);
    INFO_RELEASE("h30082497 HashMapStateBackend:createOperatorStateBackend end");
    return builder.build();
}

void HashMapStateBackend::restoreState()
{
    // restore states
}