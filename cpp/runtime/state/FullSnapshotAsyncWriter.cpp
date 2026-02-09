#include "FullSnapshotAsyncWriter.h"
#include "CheckpointStateOutputStreamProxy.h"
#include "bridge/OmniTaskBridge.h"
#include "KeyGroupRangeOffsets.h"
#include "KeyGroupsSavepointStateHandle.h"
#include <sstream>
#include "common.h"
FullSnapshotAsyncWriter::FullSnapshotAsyncWriter(
    SnapshotType *snapshotType,
    long checkpointId,
    const std::shared_ptr<FullSnapshotResources> & snapshotResources)
    : snapshotResources_(snapshotResources),checkpointId_(checkpointId),snapshotType_(snapshotType)
{
}
static constexpr int END_OF_KEY_GROUP_MASK = 0xffff;
static constexpr int FIRST_BIT_IN_BYTE_MASK = 0x80;
std::shared_ptr<SnapshotResult<KeyedStateHandle>> FullSnapshotAsyncWriter::get(
    std::shared_ptr<omnistream::OmniTaskBridge> bridge)
{
    try{
    auto keyGroupRangeOffsets = std::make_shared<KeyGroupRangeOffsets>(
        *snapshotResources_->getKeyGroupRange());
    CheckpointStateOutputStreamProxy stream(bridge, checkpointId_);
    stream.writeMetadata(snapshotResources_->getMetaInfoSnapshots());
    std::vector<int8_t> previousKey;
    std::vector<int8_t> previousValue;
    auto mergeIterator = snapshotResources_->createKVStateIterator();
    if (mergeIterator->isValid()) {
        keyGroupRangeOffsets->setKeyGroupOffset(mergeIterator->keyGroup(), stream.getPos());
        stream.writeShort(mergeIterator->kvStateId());
        previousKey = mergeIterator->key();
        previousValue = mergeIterator->value();
        mergeIterator->next();
    }
    while (mergeIterator->isValid()) {
        if (mergeIterator->isNewKeyGroup() || mergeIterator->isNewKeyValueState()) {
            previousKey[0] |= FIRST_BIT_IN_BYTE_MASK;
        }
        stream.writeInt(previousKey.size());
        stream.writeBytes(previousKey.data(), previousKey.size());
        stream.writeInt(previousValue.size());
        stream.writeBytes(previousValue.data(), previousValue.size());
        if (mergeIterator->isNewKeyGroup()) {
            stream.writeShort(END_OF_KEY_GROUP_MASK);
            keyGroupRangeOffsets->setKeyGroupOffset(mergeIterator->keyGroup(), stream.getPos());
            stream.writeShort(mergeIterator->kvStateId());
        } else if (mergeIterator->isNewKeyValueState()) {
            stream.writeShort(mergeIterator->kvStateId());
        }
        previousKey = mergeIterator->key();
        previousValue = mergeIterator->value();
        mergeIterator->next();
    }
    if (!previousKey.empty()) {
        previousKey[0] |= FIRST_BIT_IN_BYTE_MASK;
        stream.writeInt(previousKey.size());
        stream.writeBytes(previousKey.data(), previousKey.size());
        stream.writeInt(previousValue.size());
        stream.writeBytes(previousValue.data(), previousValue.size());
        stream.writeShort(END_OF_KEY_GROUP_MASK);
    }
    mergeIterator->close();
    auto handle = stream.close();
    if (handle) {
        auto jobManagerOwnedSnapshot = handle->GetJobManagerOwnedSnapshot();
        auto jmKeyedState = std::make_shared<KeyGroupsSavepointStateHandle>(
            *keyGroupRangeOffsets.get(), jobManagerOwnedSnapshot);
        return SnapshotResult<KeyedStateHandle>::Of(jmKeyedState);
    }
    return SnapshotResult<KeyedStateHandle>::Empty();
}catch(std::exception& e){
    LOG(std::string("savepoint error:")+e.what());
    return SnapshotResult<KeyedStateHandle>::Empty();
}
}