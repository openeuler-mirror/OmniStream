
#ifndef OMNISTREAM_SORTEDVECTORLONG_H
#define OMNISTREAM_SORTEDVECTORLONG_H

#include "core/typeutils/TypeSerializerSingleton.h"
#include "core/typeinfo/typeconstants.h"

using namespace omniruntime::type;

class SortedVectorLong : public TypeSerializerSingleton {
public:
    SortedVectorLong()
    {
    }

    ~SortedVectorLong() override = default;

    void* deserialize(DataInputView& source) override;

    void serialize(void* record, DataOutputSerializer& target) override;

    const char* getName() const override
    {
        return "SortedVectorLong";
    }

    virtual TypeSerializer* duplicate()
    {
        return SortedVectorLong::INSTANCE;
    }

    virtual std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration()
    { // TODO impl build serializer snapshot
        NOT_IMPL_EXCEPTION;
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::SET_LONG;
    }

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::POJO, TYPE_NAME_SORTED_VECTOR_LONG_CLASS};
        return typeJson.toJson();
    }

    void setSubBufferReusable(bool bufferReusable_) override
    {
    }

    static SortedVectorLong* INSTANCE;
};
#endif // OMNISTREAM_SORTEDVECTORLONG_H
