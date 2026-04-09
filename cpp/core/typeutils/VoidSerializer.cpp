#include "VoidSerializer.h"

void* VoidSerializer::deserialize(DataInputView& source) {
    return static_cast<void *>(new Void());
}

Object* VoidSerializer::GetBuffer() {
    if (bufferReusable) {
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return new Void();
}

VoidSerializer* VoidSerializer::INSTANCE = new VoidSerializer();