#include "StringDataSerializer.h"
#include "../data/binary/BinaryStringData.h"

void *StringDataSerializer::deserialize(DataInputView &source) {
    int len        = source.readInt();
    uint8_t *bytes = new uint8_t[len];
    source.readFully(bytes, len, 0, len);
    StringData *ret = (StringData::fromBytes(bytes, len));
    return static_cast<void *>(ret);
}

void StringDataSerializer::serialize(void *record, DataOutputSerializer &target) {
    BinaryStringData *string = static_cast<BinaryStringData *>(record);
    target.writeInt(string->getSizeInBytes());
    int offset    = string->getOffset();
    int length    = string->getSizeInBytes();
    uint8_t *byte = string->toBytes();
    byte += offset;
    for (int i = 0; i < length; i++) {
        target.writeByte((*byte));
        byte++;
    }
}

StringDataSerializer * StringDataSerializer::INSTANCE = new StringDataSerializer();