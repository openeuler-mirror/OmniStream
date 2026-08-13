#include "ExternalSerializer.h"
#include "table/typeutils/InternalSerializers.h"

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
    return new ExternalSerializer(dateType, typeSerializer, false);
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
