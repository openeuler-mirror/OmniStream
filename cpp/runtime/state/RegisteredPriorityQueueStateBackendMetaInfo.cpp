#include "RegisteredPriorityQueueStateBackendMetaInfo.h"

RegisteredPriorityQueueStateBackendMetaInfo::RegisteredPriorityQueueStateBackendMetaInfo(const StateMetaInfoSnapshot &snapshot)
    : RegisteredStateMetaInfoBase(snapshot.getName()) {
    auto valueSerializerKey = StateMetaInfoSnapshot::CommonSerializerKeys::VALUE_SERIALIZER;
    elementSerializer = snapshot.getTypeSerializer(StateMetaInfoSnapshot::commonSerializerKeyToString(valueSerializerKey));
}

std::shared_ptr<StateMetaInfoSnapshot> RegisteredPriorityQueueStateBackendMetaInfo::computeSnapshot() {
    std::unordered_map<std::string, std::string> optionsMap;
    std::unordered_map<std::string, TypeSerializer*> serializerMap;
    std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>> serializerConfigSnapshotsMap;
    serializerMap.emplace("stateSerializer", getElementSerializer());

    return std::make_shared<StateMetaInfoSnapshot>(
        name,
        StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE,
        optionsMap,
        serializerConfigSnapshotsMap,
        serializerMap);
}