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

#include "functions/RuntimeContext.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"

State *RuntimeContext::getState(StateDescriptor* stateProperties)
{
    if (dynamic_cast<StreamingRuntimeContext<Object*> *>(this) != nullptr) {
        auto valueStateDescriptor = reinterpret_cast<ValueStateDescriptor<Object*> *>(stateProperties);
        auto streamRuntimeContext = reinterpret_cast<StreamingRuntimeContext<Object*> *>(this);
        return streamRuntimeContext->template getState<Object*>(valueStateDescriptor);
    } else {
        NOT_IMPL_EXCEPTION
    }
}

State *RuntimeContext::getMapState(StateDescriptor* descriptor)
{
    if (dynamic_cast<StreamingRuntimeContext<Object*> *>(this) != nullptr) {
        auto mapStateDescriptor = reinterpret_cast<MapStateDescriptor<Object*, Object*> *>(descriptor);
        auto streamRuntimeContext = reinterpret_cast<StreamingRuntimeContext<Object*> *>(this);
        return streamRuntimeContext->getMapState(mapStateDescriptor);
    } else {
        NOT_IMPL_EXCEPTION
    }
}

MapStateWrapper* RuntimeContext::getMapState(MapStateDescriptorWrapper* stateProperties)
{
    State *state = getMapState(stateProperties->getMapStateDescriptor());
    auto mapState = dynamic_cast<MapState<Object*, Object*>*>(state);
    if (mapState == nullptr) {
        THROW_RUNTIME_ERROR("Runtime getMapState is nullptr")
    }
    return new MapStateWrapper(mapState);
}

ValueStateWrapper* RuntimeContext::getState(ValueStateDescriptorWrapper* stateProperties)
{
    State *state = getState(stateProperties->getValueStateDescriptor());
    auto valueState = dynamic_cast<ValueState<Object*>*>(state);
    if (valueState == nullptr) {
        THROW_RUNTIME_ERROR("Runtime getState is nullptr")
    }
    return new ValueStateWrapper(valueState);
}
