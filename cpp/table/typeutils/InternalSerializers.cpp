#include "InternalSerializers.h"

#include "../../core/typeutils/LongSerializer.h"
#include "../types/logical/RowType.h"
#include "RowDataSerializer.h"
#include "StringDataSerializer.h"

using namespace omniruntime::type;

TypeSerializer *InternalSerializers::create(LogicalType* type) {
    return createInternal(type);
}

TypeSerializer *InternalSerializers::createInternal(LogicalType* type) {
    switch (type->getTypeId()) {
        case DataTypeId::OMNI_CONTAINER:
            return new RowDataSerializer(static_cast<RowType *>(type));
        case DataTypeId::OMNI_LONG:
            return LongSerializer::INSTANCE; // `LongSerializer` is currently dummy, we use `RowDataSerializer`'s `serialize` and `deserialize` for now
        case DataTypeId::OMNI_INT:
            return LongSerializer::INSTANCE; // `LongSerializer` is currently dummy, we use `RowDataSerializer`'s `serialize` and `deserialize` for now
        case DataTypeId::OMNI_TIME_WITHOUT_TIME_ZONE:
            return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_TIMESTAMP_WITH_TIME_ZONE:
            return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            return LongSerializer::INSTANCE;
        case DataTypeId::OMNI_VARCHAR:
            return StringDataSerializer::INSTANCE;
        default:
            THROW_LOGIC_EXCEPTION("Unknown type" + std::to_string(type->getTypeId()));
    }
}
