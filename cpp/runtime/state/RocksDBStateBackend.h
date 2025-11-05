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

#ifndef OMNISTREAM_ROCKSDBSTATEBACKEND
#define OMNISTREAM_ROCKSDBSTATEBACKEND

#include "EmbeddedRocksDBStateBackend.h"
#include <set>
#include "runtime/state/StateBackend.h"

class RocksDBStateBackend : public StateBackend {
public:
    explicit RocksDBStateBackend(TernaryBoolean enableIncrementalCheckpoint)
    {
        rocksDbStateBackend = new EmbeddedRocksDBStateBackend(enableIncrementalCheckpoint);
    };

    explicit RocksDBStateBackend(TaskInformationPOD taskConfiguration)
    {
        rocksDbStateBackend = new EmbeddedRocksDBStateBackend(taskConfiguration);
    };
    ~RocksDBStateBackend();

    template <typename K>
    AbstractKeyedStateBackend<K> *createKeyedStateBackend(
            omnistream::EnvironmentV2 *env,
            std::set<KeyedStateHandle> stateHandles,
            KeyGroupRange *keyGroupRange,
            TypeSerializer *keySerializer,
            int numberOfKeyGroups) {}

    template <typename K>
    AbstractKeyedStateBackend<K> *createKeyedStateBackend(
        omnistream::EnvironmentV2 *env,
        std::string operatorIdentifier,
        std::set<std::shared_ptr<KeyedStateHandle>> stateHandles,
        KeyGroupRange *keyGroupRange,
        TypeSerializer *keySerializer,
        int numberOfKeyGroups,
        int alternativeIdx)
    {
        return rocksDbStateBackend->template createKeyedStateBackend<K>(
            env,
            operatorIdentifier,
            stateHandles,
            keyGroupRange,
            keySerializer,
            numberOfKeyGroups,
            alternativeIdx);
    };

private:
    EmbeddedRocksDBStateBackend *rocksDbStateBackend;
};

#endif // OMNISTREAM_ROCKSDBSTATEBACKEND