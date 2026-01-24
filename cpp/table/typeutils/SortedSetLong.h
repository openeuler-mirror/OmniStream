
#ifndef OMNISTREAM_SORTEDSETLONG_H
#define OMNISTREAM_SORTEDSETLONG_H

#include "../../core/typeutils/TypeSerializerSingleton.h"
using namespace omniruntime::type;

class SortedSetLong : public TypeSerializerSingleton {
public:
    SortedSetLong() {};
    void *deserialize(DataInputView &source) override;

    void serialize(void *record, DataOutputSerializer &target) override;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::SET_LONG;
    }
};
#endif //OMNISTREAM_SORTEDSETLONG_H