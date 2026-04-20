#include "FullSnapshotAsyncWriter.h"
#include "CheckpointStateOutputStreamProxy.h"
#include "bridge/OmniTaskBridge.h"
#include "KeyGroupRangeOffsets.h"
#include "KeyGroupsSavepointStateHandle.h"
#include "KeyGroupsStateHandle.h"
#include <sstream>
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
std::shared_ptr<SnapshotResult<KeyedStateHandle>> FullSnapshotAsyncWriter::get(
    std::shared_ptr<omnistream::OmniTaskBridge> bridge)
{
    std::shared_ptr<KeyValueStateIterator> mergeIterator = nullptr;
    try{
        INFO_RELEASE("FullSnapshotAsyncWriter::get cp=" << checkpointId_
            << " start, bridge=" << (bridge ? "ok" : "null")
            << ", snapshotResources=" << (snapshotResources_ ? "ok" : "null"));
        auto *kgRange = snapshotResources_->getKeyGroupRange();
        INFO_RELEASE("FullSnapshotAsyncWriter::get cp=" << checkpointId_
            << " keyGroupRange=" << (kgRange ? "ok" : "null")
            << ", start=" << (kgRange ? kgRange->getStartKeyGroup() : -1)
            << ", end=" << (kgRange ? kgRange->getEndKeyGroup() : -1));
        auto keyGroupRangeOffsets = std::make_shared<KeyGroupRangeOffsets>(*kgRange);
        INFO_RELEASE("FullSnapshotAsyncWriter::get cp=" << checkpointId_
            << " keyGroupRangeOffsets created, checkpointOptions=" << (checkpointOptions_ ? "ok" : "null"));
        CheckpointStateOutputStreamProxy stream(bridge, checkpointId_, checkpointOptions_);
        INFO_RELEASE("FullSnapshotAsyncWriter::get cp=" << checkpointId_
            << " stream created, writeMetadata start, metaInfoCount=" << snapshotResources_->getMetaInfoSnapshots().size());
        stream.writeMetadata(snapshotResources_->getMetaInfoSnapshots(), keySerializer_);
        INFO_RELEASE("FullSnapshotAsyncWriter::get cp=" << checkpointId_
            << " writeMetadata finished");
        std::vector<int8_t> previousKey;
        std::vector<int8_t> previousValue;
        mergeIterator = snapshotResources_->createKVStateIterator();
        INFO_RELEASE("FullSnapshotAsyncWriter::get cp=" << checkpointId_
            << " createKVStateIterator finished, valid=" << (mergeIterator && mergeIterator->isValid()));
        int entryCount = 0;
        if (mergeIterator->isValid()) {
            keyGroupRangeOffsets->setKeyGroupOffset(mergeIterator->keyGroup(), stream.getPos());
            stream.writeShort(mergeIterator->kvStateId());
            previousKey = mergeIterator->key();
            previousValue = mergeIterator->value();
            mergeIterator->next();
            entryCount++;
        }
        INFO_RELEASE("FullSnapshotAsyncWriter::get cp=" << checkpointId_
            << " first entry read, starting iteration loop");
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
            entryCount++;
            if (entryCount % 10000 == 0) {
                INFO_RELEASE("FullSnapshotAsyncWriter::get cp=" << checkpointId_
                    << " iteration progress: " << entryCount << " entries written");
            }
        }
        INFO_RELEASE("FullSnapshotAsyncWriter::get cp=" << checkpointId_
            << " iteration done, total entries=" << entryCount);
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
            std::shared_ptr<KeyedStateHandle> jmKeyedState;
            const bool isSavepoint = snapshotType_ && snapshotType_->IsSavepoint();
            if (isSavepoint) {
                jmKeyedState = std::make_shared<KeyGroupsSavepointStateHandle>(
                    *keyGroupRangeOffsets.get(), jobManagerOwnedSnapshot);
            } else {
                jmKeyedState = std::make_shared<KeyGroupsStateHandle>(
                    *keyGroupRangeOffsets.get(), jobManagerOwnedSnapshot);
            }
            INFO_RELEASE("FullSnapshotAsyncWriter::get cp=" << checkpointId_
                << " isSavepoint=" << isSavepoint << " handle=ok");
            snapshotResources_->cleanup();
            return SnapshotResult<KeyedStateHandle>::Of(jmKeyedState);
        }
        INFO_RELEASE("FullSnapshotAsyncWriter::get cp=" << checkpointId_
            << " stream.close() returned null handle");
        snapshotResources_->cleanup();
        return SnapshotResult<KeyedStateHandle>::Empty();
    }catch(std::exception& e){
        if(mergeIterator) {
            mergeIterator->close();
        }
        snapshotResources_->cleanup();
        INFO_RELEASE("FullSnapshotAsyncWriter::get cp=" << checkpointId_
            << " exception: " << e.what());
        throw;
    }
}