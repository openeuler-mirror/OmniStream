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

#ifndef OMNISTREAM_OPERATORSTATEHANDLE_H
#define OMNISTREAM_OPERATORSTATEHANDLE_H
#include "runtime/state/StateObject.h"
#include "StreamStateHandle.h"
#include "core/fs/FSDataInputStream.h"
#include <vector>
#include <unordered_map>

class OperatorStateHandle : public StateObject {
public:
    enum class Mode {
        SPLIT_DISTRIBUTE,
        UNION,
        BROADCAST
    };

    class StateMetaInfo {
    public:
        StateMetaInfo(std::vector<long> offsets, OperatorStateHandle::Mode distributionMode);
        std::vector<long> getOffsets();
        OperatorStateHandle::Mode getDistributionMode();

        bool operator==(const StateMetaInfo &other) const;
    private:
        std::vector<long> offsets_;
        OperatorStateHandle::Mode distributionMode_;
    };

    virtual std::unordered_map<std::string, StateMetaInfo> getStateNameToPartitionOffsets() const = 0;
    virtual FSDataInputStream *openInputStream() = 0;
    virtual StreamStateHandle *getDelegateStateHandle() = 0;
};
#endif // OMNISTREAM_OPERATORSTATEHANDLE_H
