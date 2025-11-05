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

#ifndef FLINK_TNEL_PERKEYSTATEDATAVIEWSTORE_H
#define FLINK_TNEL_PERKEYSTATEDATAVIEWSTORE_H

#include <string>
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "core/api/common/state/MapStateDescriptor.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "core/typeutils/TypeSerializer.h"
#include "StateMapView.h"
#include "StateDataViewStore.h"

template<typename K>
class PerKeyStateDataViewStore : public StateDataViewStore {
public:
    PerKeyStateDataViewStore(StreamingRuntimeContext<K> *ctx) : ctx(ctx) {};

    template <typename N, typename EK, typename EV>
    StateMapView<N, EK, EV> *getStateMapView(const std::string &stateName, bool supportNullKey,
        TypeSerializer *keySerializer, TypeSerializer *valueSerializer)
    {
        // What if it is not a heapstate?
        MapStateDescriptor<EK, EV> *mapStateDescriptor = new MapStateDescriptor<EK, EV>(stateName, keySerializer,
                                                                                        valueSerializer);
        MapState<EK, EV> *mapState = ctx->template getMapState<EK, EV>(mapStateDescriptor);
        if (supportNullKey) {
            std::string newName = stateName + "_null_state";
            ValueStateDescriptor<EV> *nullStateDescriptor = new ValueStateDescriptor<EV>(newName, valueSerializer);
            ValueState<EV> *nullState = ctx->template getState<EV>(nullStateDescriptor);
            return new KeyedStateMapViewWithKeysNullable<N, EK, EV>(mapState, nullState);
        } else {
            return new KeyedStateMapViewWithKeysNotNull<N, EK, EV>(mapState);
        }
    };

private:
    StreamingRuntimeContext<K> *ctx;
};

#endif // FLINK_TNEL_PERKEYSTATEDATAVIEWSTORE_H
