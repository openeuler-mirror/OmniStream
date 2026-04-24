#ifndef OMNISTREAM_REGISTEREDPRIORITYQUEUESTATEBACKENDMETAINFO_H
#define OMNISTREAM_REGISTEREDPRIORITYQUEUESTATEBACKENDMETAINFO_H

#include <string>
#include "RegisteredStateMetaInfoBase.h"
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/StateDescriptor.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"


class RegisteredPriorityQueueStateBackendMetaInfo : public RegisteredStateMetaInfoBase {
public:
    explicit RegisteredPriorityQueueStateBackendMetaInfo(const StateMetaInfoSnapshot &snapshot);

    RegisteredPriorityQueueStateBackendMetaInfo( const std::string& name, TypeSerializer* elementSerializer)
        : RegisteredStateMetaInfoBase(name), elementSerializer(elementSerializer) {}

    std::shared_ptr<StateMetaInfoSnapshot> snapshot() override { return computeSnapshot(); }

    TypeSerializer* getElementSerializer() { return elementSerializer; }

    void updateElementSerializer(TypeSerializer* serializer) { elementSerializer = serializer; }

private:
    TypeSerializer* elementSerializer;

    std::shared_ptr<StateMetaInfoSnapshot> computeSnapshot();
};


#endif //OMNISTREAM_REGISTEREDPRIORITYQUEUESTATEBACKENDMETAINFO_H