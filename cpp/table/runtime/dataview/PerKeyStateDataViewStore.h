#ifndef FLINK_TNEL_PERKEYSTATEDATAVIEWSTORE_H
#define FLINK_TNEL_PERKEYSTATEDATAVIEWSTORE_H
// #include "StateDataViewStore.h"
#include "../../../core/operators/StreamingRuntimeContext.h"
#include <string>
#include "../../../core/api/MapState.h"
#include "../../../core/api/ValueState.h"
#include "../../../core/api/common/state/MapStateDescriptor.h"
#include "../../../core/api/common/state/ValueStateDescriptor.h"
#include "../../../core/typeutils/TypeSerializer.h"
#include "../StateMapView.h"
#include "StateDataViewStore.h"

template<typename K>
class PerKeyStateDataViewStore : public StateDataViewStore
{
private:
    StreamingRuntimeContext<K> *ctx;

public:
    PerKeyStateDataViewStore(StreamingRuntimeContext<K> *ctx) : ctx(ctx) {};

    template <typename N, typename EK, typename EV>
    StateMapView<N, EK, EV> *getStateMapView(const std::string &stateName, bool supportNullKey, TypeSerializer *keySerializer, TypeSerializer *valueSerializer)
    {
        //TODO: What if it is not a heapstate?
        MapStateDescriptor *mapStateDescriptor = new MapStateDescriptor(stateName, keySerializer, valueSerializer);
        MapState<EK, EV> *mapState = ctx->template getMapState<EK, EV>(mapStateDescriptor);
        if (supportNullKey)
        {
            std::string newName = stateName + "_null_state";
            ValueStateDescriptor *nullStateDescriptor = new ValueStateDescriptor(newName, valueSerializer);
            ValueState<EV> *nullState = ctx->template getState<EV>(nullStateDescriptor);
            return new KeyedStateMapViewWithKeysNullable<N, EK, EV>(mapState, nullState);
        }
        else
        {
            return new KeyedStateMapViewWithKeysNotNull<N, EK, EV>(mapState);
        }
    };
};

#endif // FLINK_TNEL_PERKEYSTATEDATAVIEWSTORE_H
