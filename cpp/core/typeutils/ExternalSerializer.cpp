#include "ExternalSerializer.h"
#include <memory>
#include "core/typeutils/LongSerializer.h"
#include "table/typeutils/InternalSerializers.h"
#include "table/typeutils/StringDataSerializer.h"

ExternalSerializer::ExternalSerializer(LogicalType* dateType, TypeSerializer* internalSerializer, bool isInternalInput)
    : isInternalInput(isInternalInput),
      dateType(dateType),
      internalSerializer(internalSerializer)
{
}

BackendDataType ExternalSerializer::getBackendId() const
{
    BackendDataType internalBackendId = internalSerializer->getBackendId();
    switch (internalBackendId) {
        case BackendDataType::BIGINT_BK: return BackendDataType::EXTERNAL_BIGINT_BK;
        default: NOT_IMPL_EXCEPTION;
    }
}

ExternalSerializer* ExternalSerializer::of(LogicalType* dateType)
{
    TypeSerializer* typeSerializer = InternalSerializers::create(dateType);
    // ExternalSerializer owns its internal serializer. Do not transfer ownership
    // of the shared instances returned by InternalSerializers::create().
    if (typeSerializer == LongSerializer::INSTANCE) {
        typeSerializer = new LongSerializer();
    } else if (typeSerializer == StringDataSerializer::INSTANCE) {
        typeSerializer = new StringDataSerializer();
    }
    std::unique_ptr<TypeSerializer> ownedSerializer(typeSerializer);
    auto serializer = new ExternalSerializer(dateType, ownedSerializer.get(), false);
    ownedSerializer.release();
    return serializer;
}

LogicalType* ExternalSerializer::getDataType()
{
    return dateType;
}

void* ExternalSerializer::deserialize(DataInputView& source)
{
    return internalSerializer->deserialize(source);
}

void ExternalSerializer::serialize(void* record, DataOutputSerializer& target)
{
    internalSerializer->serialize(record, target);
}

void ExternalSerializer::deserialize(Object* buffer, DataInputView& source)
{
    internalSerializer->deserialize(buffer, source);
}

void ExternalSerializer::serialize(Object* buffer, DataOutputSerializer& target)
{
    internalSerializer->serialize(buffer, target);
}
