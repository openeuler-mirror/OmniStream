#include "BinaryWriter.h"
#include "table/data/TimestampData.h"

using namespace omniruntime::type;

void BinaryWriter::write(BinaryWriter *writer, int pos, void *object, LogicalType* type, TypeSerializer *serializer) {
    switch (type->getTypeId()) {
        case DataTypeId::OMNI_LONG:
            writer->writeLong(pos, *(reinterpret_cast<long *>(object)));
            break;
        case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
            writer->writeLong(pos, reinterpret_cast<TimestampData *>(object)->getMillisecond());
            break;
        default:
            THROW_LOGIC_EXCEPTION("Unknown type" + std::to_string(type->getTypeId()));
    }
}
