#include <cstring>
#include <map>
#include <sstream>
#include <iomanip>
#include <stdexcept>
#include "FullSnapshotAsyncWriter.h"
#include "CheckpointStateOutputStreamProxy.h"
#include "bridge/OmniTaskBridge.h"
#include "KeyGroupRangeOffsets.h"
#include "KeyGroupsSavepointStateHandle.h"
#include "KeyGroupsStateHandle.h"
#include "TimerConsistencyCheckControl.h"
#include "state/heap/HeapPriorityQueueDataDigest.h"
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

/**
 * Writes key-group-prefixed key/value entries into a savepoint stream.
 *
 * <p>Supports two write paths:
 * <ul>
 * <li>Patchable: writes directly into the stream buffer with a BytePatch
 *     handle. The first byte of the key can later be patched with
 *     {@code FIRST_BIT_IN_BYTE_MASK} to mark end-of-key-group/state.</li>
 * <li>Buffered: when the entry is too large for the stream chunk, the
 *     key/value are copied into pending vectors and written later via
 *     {@link CheckpointStateOutputStreamProxy#writeKeyValuePair}.</li>
 * </ul>
 *
 * <p>Usage pattern: call {@code writeFirst} for the first entry, then
 * {@code writeNext} for subsequent entries, and finally {@code finish}.
 * Between calls the writer maintains a pending entry that must be finalized
 * before any operation that may flush or reallocate the stream buffer.
 */
class SavepointKvStreamWriter {
public:
    SavepointKvStreamWriter(
        CheckpointStateOutputStreamProxy& stream,
        KeyGroupRangeOffsets& keyGroupRangeOffsets)
        : stream_(stream),
          keyGroupRangeOffsets_(keyGroupRangeOffsets)
    {}

    void writeFirst(ByteView key, ByteView value, int kvStateId, int keyGroup)
    {
        keyGroupRangeOffsets_.setKeyGroupOffset(keyGroup, stream_.getPos());
        stream_.writeShort(kvStateId);
        stream_.prepareForPatchableKeyValuePair(encodedLength(key, value));
        writePending(key, value);
    }

    void writeNext(ByteView key, ByteView value, int kvStateId, int keyGroup, bool newKeyGroup, bool newKvState)
    {
        // A patchable pending KV owns a BytePatch into the current stream buffer.
        // It must be finalized and released before any prepare call that may flush
        // or reallocate the stream buffer.
        finalizePending(newKeyGroup || newKvState);
        if (newKeyGroup) {
            stream_.writeShort(END_OF_KEY_GROUP_MASK);
            keyGroupRangeOffsets_.setKeyGroupOffset(keyGroup, stream_.getPos());
            stream_.writeShort(kvStateId);
        } else if (newKvState) {
            stream_.writeShort(kvStateId);
        }
        stream_.prepareForPatchableKeyValuePair(encodedLength(key, value));
        writePending(key, value);
    }

    void finish()
    {
        if (!pending_.hasPending) {
            return;
        }
        finalizePending(true);
        stream_.writeShort(END_OF_KEY_GROUP_MASK);
    }

private:
    struct PendingEntry {
        bool hasPending = false;
        bool buffered = false;
        CheckpointStateOutputStreamProxy::BytePatch patch;
        std::vector<int8_t> key;
        std::vector<int8_t> value;
    };

    CheckpointStateOutputStreamProxy& stream_;
    KeyGroupRangeOffsets& keyGroupRangeOffsets_;
    PendingEntry pending_;

    static void copyViewToVector(std::vector<int8_t>& target, ByteView source)
    {
        target.resize(source.size());
        if (!source.empty()) {
            std::memcpy(target.data(), source.data(), source.size());
        }
    }

    static void markEndOfKeyGroupOrState(std::vector<int8_t>& key)
    {
        key[0] = static_cast<int8_t>(static_cast<uint8_t>(key[0]) | FIRST_BIT_IN_BYTE_MASK);
    }

    static size_t encodedLength(ByteView key, ByteView value)
    {
        return sizeof(int32_t) + key.size() + sizeof(int32_t) + value.size();
    }

    void writePending(ByteView key, ByteView value)
    {
        if (key.empty()) {
            throw std::runtime_error("Savepoint key/value entry key must not be empty");
        }
        CheckpointStateOutputStreamProxy::BytePatch patch;
        bool wrotePatchable = stream_.tryWritePatchableKeyValuePair(key, value, patch);
        if (wrotePatchable) {
            pending_.patch = patch;
            pending_.buffered = false;
        } else {
            copyViewToVector(pending_.key, key);
            copyViewToVector(pending_.value, value);
            pending_.buffered = true;
            pending_.patch = {};
        }
        pending_.hasPending = true;
    }

    void finalizePending(bool markEnd)
    {
        if (!pending_.hasPending) {
            return;
        }
        if (pending_.buffered) {
            if (markEnd) {
                markEndOfKeyGroupOrState(pending_.key);
            }
            stream_.writeKeyValuePair(pending_.key, pending_.value);
            pending_.key.clear();
            pending_.value.clear();
        } else if (markEnd) {
            stream_.patchByte(pending_.patch, FIRST_BIT_IN_BYTE_MASK);
        }
        if (!pending_.buffered) {
            stream_.releasePatch(pending_.patch);
        }
        pending_.hasPending = false;
        pending_.patch = {};
    }
};

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
                                     ByteView key,
                                     ByteView value) {
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

        mergeIterator = snapshotResources_->createKVStateIterator();
        SavepointKvStreamWriter kvStreamWriter(
            stream,
            *keyGroupRangeOffsets);
        if (mergeIterator->isValid()) {
            const auto& entry = mergeIterator->current();
            if (shouldDigestHeapPq) {
                recordHeapPqEntry(entry.kvStateId, entry.key, entry.value);
            }
            kvStreamWriter.writeFirst(entry.key, entry.value, entry.kvStateId, entry.keyGroup);
            mergeIterator->next();
        }
        while (mergeIterator->isValid()) {
            const auto& entry = mergeIterator->current();
            if (shouldDigestHeapPq) {
                recordHeapPqEntry(entry.kvStateId, entry.key, entry.value);
            }
            kvStreamWriter.writeNext(
                entry.key,
                entry.value,
                entry.kvStateId,
                entry.keyGroup,
                entry.newKeyGroup,
                entry.newKeyValueState);
            mergeIterator->next();
        }
        kvStreamWriter.finish();
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
            return SnapshotResult<KeyedStateHandle>::Of(jmKeyedState);
        }
        for (const auto &digest : heapPqDigests) {
            HeapPriorityQueueDataDigest::logSummary(digest.second);
        }
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
