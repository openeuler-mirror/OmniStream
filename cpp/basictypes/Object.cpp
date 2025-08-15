#include "basictypes/Object.h"

Object::Object() = default;

Object::Object(nlohmann::json jsonObj) {
    return;
}

Object::~Object() = default;

int Object::hashCode() { return 0; }

bool Object::equals(Object *obj) { return false; }

std::string Object::toString() { return std::string(); }

Object *Object::clone() { return nullptr; }

Object::Object(const Object &obj){
    this->refCount = obj.refCount;
    this->isClone = obj.isClone;
}

Object::Object(Object &&obj){
    this->refCount = obj.refCount;
    this->isClone = obj.isClone;
}

Object &Object::operator=(const Object &obj){
    this->refCount = obj.refCount;
    this->isClone = obj.isClone;
    return *this;
}

Object &Object::operator=(Object &&obj){
    this->refCount = obj.refCount;
    this->isClone = obj.isClone;
    return *this;
}



void Object::putRefCount() {
    if (__builtin_expect(--refCount != 0, true)) {
        return;
    }
    delete this;
}

void Object::getRefCount() {
    ++refCount;
}

void Object::setRefCount(uint32_t count) {
    refCount = count;
}

bool Object::isCloned() {
    return isClone;
}

uint32_t Object::getRefCountNumber() {
    return refCount;
}
