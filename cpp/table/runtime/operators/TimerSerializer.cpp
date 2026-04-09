#include "TimerSerializer.h"
#include "core/utils/MathUtils.h"

TimerSerializer::TimerSerializer(TypeSerializer* keySerializer_, TypeSerializer* namespaceSerializer_, Class* keyClazz_, Class* namespaceClazz_)
: keySerializer(keySerializer_), namespaceSerializer(namespaceSerializer_), keyClazz(keyClazz_), namespaceClazz(namespaceClazz_) {
    reuseBuffer = static_cast<Object*>(createInstance());
    setSubBufferReusable(false);
}

TimerSerializer::~TimerSerializer() {
    if (keySerializer != nullptr) {
        delete keySerializer;
    }
    if (namespaceSerializer != nullptr) {
        delete namespaceSerializer;
    }
    if (keyClazz != nullptr) {
        delete keyClazz;
    }
    if (namespaceClazz != nullptr) {
        delete namespaceClazz;
    }
}

TimerHeapInternalTimer<Object*, Object*>* TimerSerializer::createInstance() {
    return new TimerHeapInternalTimer<Object*, Object*>(0L, keyClazz->newInstance(), namespaceClazz->newInstance());
}

void TimerSerializer::serialize(Object* buffer, DataOutputSerializer& target) {
    auto timer = static_cast<TimerHeapInternalTimer<Object*, Object*>*>(buffer);
    target.writeLong(MathUtils::flipSignBit(timer->getTimestamp()));
    keySerializer->serialize(timer->getKey(), target);
    namespaceSerializer->serialize(timer->getNamespace(), target);
}

void TimerSerializer::deserialize(Object* buffer, DataInputView& source) {
    auto timer = static_cast<TimerHeapInternalTimer<Object*, Object*>*>(buffer);
    long timestamp = MathUtils::flipSignBit(source.readLong());

    // deserialize key
    auto keyBuffer = keySerializer->GetBuffer();
    if (keyBuffer != nullptr) {
        keySerializer->deserialize(keyBuffer, source);
        // Update the timer's key
        timer->setKey(keyBuffer);
        keyBuffer->putRefCount();
    }

    // deserialize namespace
    auto namespaceBuffer = namespaceSerializer->GetBuffer();
    if (namespaceBuffer != nullptr) {
        namespaceSerializer->deserialize(namespaceBuffer, source);
        // Update the timer's namespace
        timer->setNamespace(namespaceBuffer);
        namespaceBuffer->putRefCount();
    }

    // set the timestamp
    timer->setTimestamp(timestamp);
}

void TimerSerializer::setSubBufferReusable(bool bufferReusable_) {
    keySerializer->setSelfBufferReusable(bufferReusable_);
    namespaceSerializer->setSelfBufferReusable(bufferReusable_);
}

Object* TimerSerializer::GetBuffer() {
    if (bufferReusable) {
        reuseBuffer->getRefCount();
        return reuseBuffer;
    }
    return static_cast<Object*>(createInstance());
}