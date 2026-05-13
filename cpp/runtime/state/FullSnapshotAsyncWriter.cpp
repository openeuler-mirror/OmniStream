#include "FullSnapshotAsyncWriter.h"
#include "CheckpointStateOutputStreamProxy.h"
#include "bridge/OmniTaskBridge.h"
#include "KeyGroupRangeOffsets.h"
#include "KeyGroupsSavepointStateHandle.h"
#include "KeyGroupsStateHandle.h"
#include <sstream>
#include <iomanip>
#include "common.h"
FullSnapshotAsyncWriter::FullSnapshotAsyncWriter(
    SnapshotType *snapshotType,
    CheckpointOptions *checkpointOptions,
    long checkpointId,
    const std::shared_ptr<FullSnapshotResources> & snapshotResources,
    std::string keySerializer)
    : snapshotResources_(snapshotResources),
    checkpointOptions_(checkpointOptions),
    checkpointId_(checkpointId),
    snapshotType_(snapshotType),
    keySerializer_(keySerializer)
{
}
static constexpr int END_OF_KEY_GROUP_MASK = 0xffff;
static constexpr int FIRST_BIT_IN_BYTE_MASK = 0x80;

static std::string hexStr(const std::vector<int8_t>& data) {
    if (data.empty()) return "[]";
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < data.size(); ++i) {
        if (i > 0) oss << " ";
        oss << std::hex << std::setw(2) << std::setfill('0')
            << (static_cast<unsigned int>(static_cast<uint8_t>(data[i])));
    }
    oss << "]";
    return oss.str();
}

std::shared_ptr<SnapshotResult<KeyedStateHandle>> FullSnapshotAsyncWriter::get(
    std::shared_ptr<omnistream::OmniTaskBridge> bridge)
{
    std::shared_ptr<KeyValueStateIterator> mergeIterator = nullptr;
    try{
        auto *kgRange = snapshotResources_->getKeyGroupRange();
        auto keyGroupRangeOffsets = std::make_shared<KeyGroupRangeOffsets>(*kgRange);
        CheckpointStateOutputStreamProxy stream(bridge, checkpointId_, checkpointOptions_);
        stream.writeMetadata(snapshotResources_->getMetaInfoSnapshots(), keySerializer_);
        std::vector<int8_t> previousKey;
        std::vector<int8_t> previousValue;
        mergeIterator = snapshotResources_->createKVStateIterator();
        int entryIdx = 0;
        if (mergeIterator->isValid()) {
            keyGroupRangeOffsets->setKeyGroupOffset(mergeIterator->keyGroup(), stream.getPos());
            stream.writeShort(mergeIterator->kvStateId());
            previousKey = mergeIterator->key();
            previousValue = mergeIterator->value();
            INFO_RELEASE("savepoint: FullSnapshotAsyncWriter entry " << entryIdx
                << " kg=" << mergeIterator->keyGroup()
                << " kvId=" << mergeIterator->kvStateId()
                << " key=" << hexStr(previousKey)
                << " val=" << hexStr(previousValue));
            entryIdx++;
            mergeIterator->next();
        }
        while (mergeIterator->isValid()) {
            if (mergeIterator->isNewKeyGroup() || mergeIterator->isNewKeyValueState()) {
                INFO_RELEASE("savepoint: FullSnapshotAsyncWriter entry " << entryIdx
                << " kg=" << mergeIterator->keyGroup()
                << " kvId=" << mergeIterator->kvStateId()
                << " newKG=" << (mergeIterator->isNewKeyGroup() ? 1 : 0)
                << " newKVS=" << (mergeIterator->isNewKeyValueState() ? 1 : 0)
                << " prevKey=" << hexStr(previousKey)
                << " prevVal=" << hexStr(previousValue));
                previousKey[0] |= FIRST_BIT_IN_BYTE_MASK;

            }

            entryIdx++;
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
            INFO_RELEASE("savepoint: FullSnapshotAsyncWriter final entry " << entryIdx
            << " prevKey=" << hexStr(previousKey)
            << " prevVal=" << hexStr(previousValue));
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
            std::shared_ptr<KeyedStateHandle> jmKeyedState;
            const bool isSavepoint = snapshotType_ && snapshotType_->IsSavepoint();
            if (isSavepoint) {
                jmKeyedState = std::make_shared<KeyGroupsSavepointStateHandle>(
                    *keyGroupRangeOffsets.get(), jobManagerOwnedSnapshot);
            } else {
                jmKeyedState = std::make_shared<KeyGroupsStateHandle>(
                    *keyGroupRangeOffsets.get(), jobManagerOwnedSnapshot);
            }
            snapshotResources_->cleanup();
            return SnapshotResult<KeyedStateHandle>::Of(jmKeyedState);
        }
        snapshotResources_->cleanup();
        return SnapshotResult<KeyedStateHandle>::Empty();
    }catch(std::exception& e){
        if(mergeIterator) {
            mergeIterator->close();
        }
        snapshotResources_->cleanup();
        INFO_RELEASE("Error:FullSnapshotAsyncWriter::get cp=" << checkpointId_
            << " exception: " << e.what());
        throw;
    }
}