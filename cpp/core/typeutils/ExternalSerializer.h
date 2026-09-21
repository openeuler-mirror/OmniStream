#ifndef OMNISTREAM_EXTERNALSERIALIZER_H
#define OMNISTREAM_EXTERNALSERIALIZER_H

#include "TypeSerializer.h"
#include "SerializerJsonInfo.h"

class ExternalSerializer : public TypeSerializer {
public:
    ExternalSerializer(LogicalType* dateType, TypeSerializer* internalSerializer,bool isInternalInput);
    static ExternalSerializer* of(LogicalType* dateType);
    LogicalType* getDataType();
    void* deserialize(DataInputView& source) override;
    void serialize(void* record, DataOutputSerializer& target) override;
    void deserialize(Object* buffer, DataInputView& source) override;
    void serialize(Object* buffer, DataOutputSerializer& target) override;
    BackendDataType getBackendId() const override;
private:
    bool isInternalInput;
    LogicalType* dateType;
    TypeSerializer* internalSerializer;
};

#endif // OMNISTREAM_EXTERNALSERIALIZER_H
