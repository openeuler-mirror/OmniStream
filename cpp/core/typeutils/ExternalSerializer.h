#ifndef OMNISTREAM_EXTERNALSERIALIZER_H
#define OMNISTREAM_EXTERNALSERIALIZER_H

#include "TypeSerializerSingleton.h"

class ExternalSerializer : public TypeSerializerSingleton {
public:
    ExternalSerializer(LogicalType* dateType, TypeSerializer* internalSerializer, bool isInternalInput);
    ~ExternalSerializer() override
    {
        if (internalSerializer != nullptr) {
            delete internalSerializer;
            internalSerializer = nullptr;
        }
    }
    static ExternalSerializer* of(LogicalType* dateType);
    LogicalType* getDataType();
    void* deserialize(DataInputView& source) override;
    void serialize(void* record, DataOutputSerializer& target) override;
    void deserialize(Object* buffer, DataInputView& source) override;
    void serialize(Object* buffer, DataOutputSerializer& target) override;
    BackendDataType getBackendId() const override;

    std::string toJson() override
    {
        SerializerJsonInfo typeJson = {SerializerType::EXTERNAL};
        typeJson.logicalType = dateType;
        typeJson.valueSerializer = internalSerializer;

        SerializerAttributes serializerAttributes;
        serializerAttributes.externalIsInternalInput = isInternalInput;
        typeJson.serializerAttributes = &serializerAttributes;

        return typeJson.toJson();
    }

private:
    bool isInternalInput;
    LogicalType* dateType;
    TypeSerializer* internalSerializer;
};

#endif // OMNISTREAM_EXTERNALSERIALIZER_H
