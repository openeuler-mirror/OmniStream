#ifndef FLINK_TNEL_STATEDATAVIEWSTORE_H
#define FLINK_TNEL_STATEDATAVIEWSTORE_H
#include "../StateMapView.h"
#include "core/typeutils/TypeSerializer.h"

class StateDataViewStore
{
public:
    template <typename N, typename EK, typename EV>
    StateMapView<N, EK, EV> getStateMapView(const std::string &stateName, bool supportNullKey, TypeSerializer *keySerializer, TypeSerializer *valueSerializer) {};
};

#endif // FLINK_TNEL_STATEDATAVIEWSTORE_H