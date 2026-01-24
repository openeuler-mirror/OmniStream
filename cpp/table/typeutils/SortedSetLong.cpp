#include "SortedSetLong.h"
#include <set>
void* SortedSetLong::deserialize(DataInputView& source)
{

    int eleNum = source.readInt();
    std::set<long> * setPtr = new std::set<long>();
    for (int i = 0; i < eleNum; i++) {
        long value = source.readLong();
        setPtr->insert(value);
    }
    return setPtr;
}


void SortedSetLong::serialize(void* record, DataOutputSerializer& target)
{
    std::set<long>* setPtr = static_cast<std::set<long>*>(record);
    int size = setPtr->size();
    target.writeInt(size);
    for (const long & value : *setPtr) {
        target.writeLong(value);
    }
}