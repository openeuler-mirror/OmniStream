
#ifndef OMNISTREAM_SORTEDVECTORLONG_H
#define OMNISTREAM_SORTEDVECTORLONG_H

#include "../../core/typeutils/TypeSerializerSingleton.h"
using namespace omniruntime::type;

class SortedVectorLong : public TypeSerializerSingleton {
public:
    SortedVectorLong() {};
    void *deserialize(DataInputView &source) override;

    void serialize(void *record, DataOutputSerializer &target) override;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::SET_LONG;
    }
};
#endif //OMNISTREAM_SORTEDVECTORLONG_H
