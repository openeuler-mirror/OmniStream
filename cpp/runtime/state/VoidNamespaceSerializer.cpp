#include "VoidNamespaceSerializer.h"

void* VoidNamespaceSerializer::deserialize(DataInputView& source) {
    return static_cast<void *>(new VoidNamespace());
}

Object* VoidNamespaceSerializer::GetBuffer() {
    if (bufferReusable) {
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return new VoidNamespace();
}

VoidNamespaceSerializer* VoidNamespaceSerializer::INSTANCE = new VoidNamespaceSerializer();