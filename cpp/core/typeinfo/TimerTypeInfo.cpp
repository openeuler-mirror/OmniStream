#include "TimerTypeInfo.h"
#include "table/runtime/operators/TimerSerializer.h"

TimerTypeInfo::TimerTypeInfo(TypeInformation* keyTypeInfo_, TypeInformation* namespaceTypeInfo_, Class* keyClazz_, Class* namespaceClazz_)
	: keyTypeInfo(keyTypeInfo_), namespaceTypeInfo(namespaceTypeInfo_), keyClazz(keyClazz_), namespaceClazz(namespaceClazz_) {
	keyTypeInfo_->getRefCount();
	namespaceTypeInfo->getRefCount();

	keyClazz_->getRefCount();
	namespaceClazz_->getRefCount();
}

TimerTypeInfo::~TimerTypeInfo() {
	keyTypeInfo->putRefCount();
	namespaceTypeInfo->putRefCount();

	keyClazz->putRefCount();
	namespaceClazz->putRefCount();
}

TypeSerializer* TimerTypeInfo::createTypeSerializer() {
    return new TimerSerializer(keyTypeInfo->createTypeSerializer(), namespaceTypeInfo->createTypeSerializer(), keyClazz, namespaceClazz);
}
