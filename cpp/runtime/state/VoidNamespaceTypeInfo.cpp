#include "VoidNamespaceTypeInfo.h"
#include "VoidNamespaceSerializer.h"

VoidNamespaceTypeInfo::VoidNamespaceTypeInfo(const char* name) : name_(name) {}

TypeSerializer* VoidNamespaceTypeInfo::createTypeSerializer() { return new VoidNamespaceSerializer(); }

std::string VoidNamespaceTypeInfo::name() { return name_; }