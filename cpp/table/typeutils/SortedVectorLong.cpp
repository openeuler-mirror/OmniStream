#include "SortedVectorLong.h"

#include <vector>

void* SortedVectorLong::deserialize(DataInputView& source)
{
    int eleNum = source.readInt();
    std::vector<long>* vecPtr = new std::vector<long>();
    vecPtr->reserve(eleNum);
    for (int i = 0; i < eleNum; i++) {
        long value = source.readLong();
        vecPtr->push_back(value);
    }
    return vecPtr;
}

void SortedVectorLong::serialize(void* record, DataOutputSerializer& target)
{
    std::vector<long>* vecPtr = static_cast<std::vector<long>*>(record);
    int size = vecPtr->size();
    target.writeInt(size);
    for (const long& value : *vecPtr) {
        target.writeLong(value);
    }
}
