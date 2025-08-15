#include "BinaryStringData.h"

BinaryStringData *BinaryStringData::EMPTY_UTF8 = fromBytes(StringUtf8Utils::encodeUTF8(new std::u32string(U"")), 0);

void BinaryStringData::ensureMaterialized()
{
    if (materialized == false)
    {
        binarySection = materialize(nullptr);
        materialized = true;
    }
}

BinarySection *BinaryStringData::materialize(TypeSerializer *S)
{
    if (S != nullptr)
    {
        THROW_LOGIC_EXCEPTION("BinaryStringData does not support custom serializers");
    }

    uint8_t *bytes = StringUtf8Utils::encodeUTF8(object);
    int size = StringUtf8Utils::computeUTF8Length(object);

    // Create a singleton array of MemorySegment
    // MemorySegment **seg = new MemorySegment *[1];
    // seg[0] = new MemorySegment(bytes, size);
    // return new BinarySection(seg, 1, 0, size);
    return new BinarySection(bytes, 0, size);
}

std::u32string *BinaryStringData::toString()
{
    if (object == nullptr)
    {
        object = StringUtf8Utils::decodeUTF8(toBytes(), 0, getSizeInBytes());
    }
    if (object->empty())
    {
        object = StringUtf8Utils::decodeUTF8(toBytes(), 0, getSizeInBytes());
    }
    return object;
}

std::string *BinaryStringData::ToUtF8String()
{
    if (!materialized) {
        // Object is already materialized (as std::u32string), encode to UTF-8 and construct std::string
        uint8_t* utf8Bytes = StringUtf8Utils::encodeUTF8(object);
        int utf8Len = StringUtf8Utils::computeUTF8Length(object);
        return new std::string(static_cast<const char*>(static_cast<const void*>(utf8Bytes)), utf8Len);
    }
    // Raw UTF-8 bytes already available — construct std::string directly
    return new std::string(static_cast<const char*>(static_cast<const void*>(toBytes())), getSizeInBytes());
}


uint8_t *BinaryStringData::toBytes() {

        ensureMaterialized();
        return getSegment()+getOffset();
    // return getSegments()[0]->getAll() + getOffset();
}

BinaryStringData *BinaryStringData::fromString(std::u32string *str)
{
    if (str->empty())
    {
        return nullptr;
    }
    else
    {
        return new BinaryStringData(str);
    }
}

BinaryStringData *BinaryStringData::fromBytes(uint8_t *bytes, int len)
{
    return fromBytes(bytes, 0, len);
}

BinaryStringData *BinaryStringData::fromBytes(uint8_t *bytes, int offset, int len)
{
    // MemorySegment **seg = new MemorySegment *[1];
    // seg[0] = new MemorySegment(bytes, len);
    // return new BinaryStringData(seg, offset, len);
    return new BinaryStringData(bytes, offset, len);
}

// BinaryStringData* BinaryStringData::fromAddress(MemorySegment* segment, int offset, int numBytes) {
//     MemorySegment** segArray = new MemorySegment*[1];
//     segArray[0] = segment;
//     BinaryStringData *tmp = new BinaryStringData(segArray, offset, numBytes);
//     return tmp;
// }