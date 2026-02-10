#include "KeyGroupsSavepointStateHandle.h"
#include "state/KeyGroupsStateHandle.h"
#include <sstream>
#include <memory>
KeyGroupsSavepointStateHandle::KeyGroupsSavepointStateHandle(
    const KeyGroupRangeOffsets& groupRangeOffsets,
    const std::shared_ptr<StreamStateHandle>& streamStateHandle)
    : KeyGroupsStateHandle(groupRangeOffsets, streamStateHandle)
{
}

KeyGroupsSavepointStateHandle::KeyGroupsSavepointStateHandle(const nlohmann::json &description)
    : KeyGroupsStateHandle(description)
{
}

std::shared_ptr<KeyedStateHandle>
KeyGroupsSavepointStateHandle::GetIntersection(const KeyGroupRange& keyGroupRange) const
{
    auto offsets = getGroupRangeOffsets().getIntersection(keyGroupRange);
    if(offsets.getKeyGroupRange().getNumberOfKeyGroups() <= 0) {
        return nullptr;
    }
    return std::make_shared<KeyGroupsSavepointStateHandle>(
        offsets,
        getDelegateStateHandle());
}
std::string KeyGroupsSavepointStateHandle::ToString() const
{
    nlohmann::json json;
    json["stateHandleName"] = "KeyGroupsSavepointStateHandle";
    json["streamStateHandle"] = nlohmann::json::parse(getDelegateStateHandle()->ToString());
    json["stateHandleID"] = nlohmann::json::parse(GetStreamStateHandleID().ToString());
    json["groupRangeOffsets"] = nlohmann::json::parse(getGroupRangeOffsets().ToString());
    return json.dump();
}