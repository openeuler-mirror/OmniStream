#include "FullSnapshotAsyncWriter.h"
#include "CheckpointStateOutputStreamProxy.h"
#include "bridge/OmniTaskBridge.h"
#include "KeyGroupRangeOffsets.h"
#include "KeyGroupsSavepointStateHandle.h"
#include "KeyGroupsStateHandle.h"
#include "TimerConsistencyCheckControl.h"
#include "state/heap/HeapPriorityQueueDataDigest.h"
#include <map>
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

std::shared_ptr<SnapshotResult<KeyedStateHandle>> FullSnapshotAsyncWriter::get(
    std::shared_ptr<omnistream::OmniTaskBridge> bridge)
{
    std::shared_ptr<KeyValueStateIterator> mergeIterator = nullptr;
    try{
        auto *kgRange = snapshotResources_->getKeyGroupRange();
        auto keyGroupRangeOffsets = std::make_shared<KeyGroupRangeOffsets>(*kgRange);
        CheckpointStateOutputStreamProxy stream(bridge, checkpointId_, checkpointOptions_);
        const auto &metaInfoSnapshots = snapshotResources_->getMetaInfoSnapshots();
        stream.writeMetadata(metaInfoSnapshots, keySerializer_);

        std::map<int, HeapPriorityQueueDataDigest::Summary> heapPqDigests;
        const bool shouldDigestHeapPq = snapshotType_ != nullptr
            && snapshotType_->IsSavepoint()
            && TimerConsistencyCheckControl::timerConsistencyCheckEnabled;
        if (shouldDigestHeapPq) {
            for (size_t i = 0; i < metaInfoSnapshots.size(); i++) {
                const int kvStateId = static_cast<int>(i);
                if (!snapshotResources_->isHeapPriorityQueueStateId(kvStateId)) {
                    continue;
                }
                const std::string stateName =
                    metaInfoSnapshots[i] == nullptr ? "" : metaInfoSnapshots[i]->getName();
                heapPqDigests.emplace(
                    kvStateId,
                    HeapPriorityQueueDataDigest::createSummary(
                        "snapshot",
                        checkpointId_,
                        stateName,
                        kgRange == nullptr ? 0 : kgRange->getNumberOfKeyGroups()));
            }
        }

        const int keyGroupPrefixBytes = snapshotResources_->getKeyGroupPrefixBytes();
        auto recordHeapPqEntry = [&](int kvStateId,
                                     const std::vector<int8_t> &key,
                                     const std::vector<int8_t> &value) {
            auto digest = heapPqDigests.find(kvStateId);
            if (digest == heapPqDigests.end()) {
                return;
            }
            HeapPriorityQueueDataDigest::addSerializedEntry(
                digest->second,
                key,
                value,
                keyGroupPrefixBytes);
        };

        std::vector<int8_t> previousKey;
        std::vector<int8_t> previousValue;
        int previousKvStateId = -1;
        size_t writtenEntries = 0;
        mergeIterator = snapshotResources_->createKVStateIterator();
        if (mergeIterator->isValid()) {
            keyGroupRangeOffsets->setKeyGroupOffset(mergeIterator->keyGroup(), stream.getPos());
            stream.writeShort(mergeIterator->kvStateId());
            previousKey = mergeIterator->key();
            previousValue = mergeIterator->value();
            previousKvStateId = mergeIterator->kvStateId();
            mergeIterator->next();
        }
        while (mergeIterator->isValid()) {
            if (shouldDigestHeapPq) {
                recordHeapPqEntry(previousKvStateId, previousKey, previousValue);
            }
            if (mergeIterator->isNewKeyGroup() || mergeIterator->isNewKeyValueState()) {
                previousKey[0] |= FIRST_BIT_IN_BYTE_MASK;
            }
            stream.writeInt(previousKey.size());
            stream.writeBytes(previousKey.data(), previousKey.size());
            stream.writeInt(previousValue.size());
            stream.writeBytes(previousValue.data(), previousValue.size());
            writtenEntries++;
            if (mergeIterator->isNewKeyGroup()) {
                stream.writeShort(END_OF_KEY_GROUP_MASK);
                keyGroupRangeOffsets->setKeyGroupOffset(mergeIterator->keyGroup(), stream.getPos());
                stream.writeShort(mergeIterator->kvStateId());
            } else if (mergeIterator->isNewKeyValueState()) {
                stream.writeShort(mergeIterator->kvStateId());
            }
            previousKey = mergeIterator->key();
            previousValue = mergeIterator->value();
            previousKvStateId = mergeIterator->kvStateId();
            mergeIterator->next();
        }
        if (!previousKey.empty()) {
            if (shouldDigestHeapPq) {
                recordHeapPqEntry(previousKvStateId, previousKey, previousValue);
            }
            previousKey[0] |= FIRST_BIT_IN_BYTE_MASK;
            stream.writeInt(previousKey.size());
            stream.writeBytes(previousKey.data(), previousKey.size());
            stream.writeInt(previousValue.size());
            stream.writeBytes(previousValue.data(), previousValue.size());
            stream.writeShort(END_OF_KEY_GROUP_MASK);
            writtenEntries++;
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
            for (const auto &digest : heapPqDigests) {
                HeapPriorityQueueDataDigest::logSummary(digest.second);
            }
            INFO_RELEASE("[OS-CP-heap-writer] checkpointId=" << checkpointId_
                << ", metaInfoCount=" << metaInfoSnapshots.size()
                << ", writtenEntries=" << writtenEntries
                << ", keyGroupRange=" << (kgRange == nullptr ? "null" : kgRange->ToString())
                << ", streamPos=" << stream.getPos());
            return SnapshotResult<KeyedStateHandle>::Of(jmKeyedState);
        }
        for (const auto &digest : heapPqDigests) {
            HeapPriorityQueueDataDigest::logSummary(digest.second);
        }
        INFO_RELEASE("[OS-CP-heap-writer] checkpointId=" << checkpointId_
            << ", metaInfoCount=" << metaInfoSnapshots.size()
            << ", writtenEntries=" << writtenEntries
            << ", result=empty");
        return SnapshotResult<KeyedStateHandle>::Empty();
    }catch(std::exception& e){
        if(mergeIterator) {
            mergeIterator->close();
        }
        INFO_RELEASE("Error:FullSnapshotAsyncWriter::get cp=" << checkpointId_
            << " exception: " << e.what());
        throw;
    }
}
