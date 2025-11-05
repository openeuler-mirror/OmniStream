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
#ifndef FLINK_TNEL_LISTSTATEDESCRIPTOR_H
#define FLINK_TNEL_LISTSTATEDESCRIPTOR_H
#include "StateDescriptor.h"
#include "ListState.h"

/**
 * T: The type of the values that can be added to the list state.
 * */
template<typename T>
class ListStateDescriptor : public StateDescriptor {
public:
    ListStateDescriptor(std::string& name, TypeSerializer *serializer) : StateDescriptor(name, serializer)
    {
        backendDataId = serializer->getBackendId();
    };
    Type getType() override { return StateDescriptor::Type::LIST; };

    ListStateDescriptor(std::string& name, TypeInformation* typeInfo) :  StateDescriptor(name, typeInfo)
    {
        backendDataId = typeInfo->getBackendId();
    };

    BackendDataType getBackendId() override { return backendDataId; }
    BackendDataType getKeyDataId() override {throw std::runtime_error("No keyDataId for a ListStateDescriptor");}
    BackendDataType getValueDataId() override {throw std::runtime_error("No valueDataId for a ListStateDescriptor");}

protected:
    BackendDataType backendDataId;
};

using DataStreamListStateDescriptor = ListStateDescriptor<Object*>;

#endif // FLINK_TNEL_LISTSTATEDESCRIPTOR_H
