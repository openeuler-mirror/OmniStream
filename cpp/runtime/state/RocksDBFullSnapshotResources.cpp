#include "RocksDBFullSnapshotResources.h"
#include "state/rocksdb/iterator/SingleStateIterator.h"
#include "state/rocksdb/iterator/RocksStatesPerKeyGroupMergeIterator.h"
#include "state/rocksdb/iterator/RocksTransformingIteratorWrapper.h"
#include <algorithm>
const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>&
RocksDBFullSnapshotResources::getMetaInfoSnapshots()
{
    return stateMetaInfoSnapshots_;
}

KeyGroupRange* RocksDBFullSnapshotResources::getKeyGroupRange()
{
    return keyGroupRange_;
}

TypeSerializer* RocksDBFullSnapshotResources::getKeySerializer()
{
    return keySerializer_;
}

std::shared_ptr<KeyValueStateIterator> RocksDBFullSnapshotResources::createKVStateIterator()
{
    auto closeableRegistry = std::make_unique<CloseableRegistry>();
    rocksdb::ReadOptions readOptions = {};
    readOptions.snapshot = snapshot_;
    std::vector<std::pair<std::unique_ptr<RocksIteratorWrapper>, int>>
        kvStateIterators = 
            createKVStateIterators(closeableRegistry, readOptions);
    return std::make_shared<RocksStatesPerKeyGroupMergeIterator>(
        std::move(closeableRegistry),
        kvStateIterators,
        heapPriorityQueueIterators_,
        keyGroupPrefixBytes_);
}

std::vector<std::pair<std::unique_ptr<RocksIteratorWrapper>, int>>
RocksDBFullSnapshotResources::createKVStateIterators(
    std::unique_ptr<CloseableRegistry>& closeableRegistry,
    const rocksdb::ReadOptions& readOptions)
{
    std::vector<std::pair<std::unique_ptr<RocksIteratorWrapper>, int>>
        kvStateIterators;
    int kvStateId = 0;
    for (auto& metaDataEntry : metaData_) {
        INFO_RELEASE("RocksDBFullSnapshotResources::createKVStateIterators kvStateId: " << kvStateId
            << ", stateName: " << metaDataEntry->rocksDbKvStateInfo->metaInfo_->getName()
            << ", cfName: " << metaDataEntry->rocksDbKvStateInfo->columnFamilyHandle_->GetName());
        auto rocksIteratorWrapper = createRocksIteratorWrapper(
            db_,
            metaDataEntry->rocksDbKvStateInfo->columnFamilyHandle_,
            metaDataEntry->stateSnapshotTransformer.get(),
            readOptions);
        kvStateIterators.push_back(std::pair{
            std::move(rocksIteratorWrapper),
            kvStateId
        });
        kvStateId++;
    }
    return kvStateIterators;
}

std::unique_ptr<RocksIteratorWrapper>
RocksDBFullSnapshotResources::createRocksIteratorWrapper(
    rocksdb::DB* db,
    rocksdb::ColumnFamilyHandle* columnFamilyHandle,
    StateSnapshotTransformer<std::vector<int8_t>>* stateSnapshotTransformer,
    const rocksdb::ReadOptions& readOptions)
{
    std::unique_ptr<rocksdb::Iterator> rocksIterator;
    rocksIterator.reset(
        db->NewIterator(readOptions,columnFamilyHandle));
    if (stateSnapshotTransformer == nullptr) {
        return std::make_unique<RocksIteratorWrapper>(std::move(rocksIterator));
    } else {
        return std::make_unique<RocksTransformingIteratorWrapper>(
            std::move(rocksIterator),
            std::move(stateSnapshotTransformer));
    }
}

RocksDBFullSnapshotResources::RocksDBFullSnapshotResources(
    ResourceGuard::Lease* lease,
    const rocksdb::Snapshot* snapshot,
    const std::vector<std::shared_ptr<RocksDbKvStateInfo>>& metaDataCopy,
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& stateMetaInfoSnapshots,
    std::vector<std::unique_ptr<SingleStateIterator>> heapPriorityQueueIterators,
    rocksdb::DB* db,
    int keyGroupPrefixBytes,
    KeyGroupRange* keyGroupRange,
    TypeSerializer* keySerializer)
    :lease_(lease),
    snapshot_(snapshot),
    stateMetaInfoSnapshots_(stateMetaInfoSnapshots),
    db_(db),
    keyGroupPrefixBytes_(keyGroupPrefixBytes),
    keyGroupRange_(keyGroupRange),
    keySerializer_(keySerializer),
    heapPriorityQueueIterators_(std::move(heapPriorityQueueIterators))
{
    for (auto& info : metaDataCopy) {
        metaData_.push_back(std::make_shared<MetaData>(info, nullptr));
    }
}

std::shared_ptr<RocksDBFullSnapshotResources>
RocksDBFullSnapshotResources::create(
    const std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>& kvStateInformation,
    const std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>& registeredPQStates,
    rocksdb::DB* db,
    const std::shared_ptr<ResourceGuard>& rocksDBResourceGuard,
    KeyGroupRange* keyGroupRange,
    TypeSerializer* keySerializer,
    int keyGroupPrefixBytes)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots;
    std::vector<std::shared_ptr<RocksDbKvStateInfo>> metaDataCopy;
    for (auto& [_, stateInfo] : kvStateInformation) {
        stateMetaInfoSnapshots.push_back(stateInfo->metaInfo_->snapshot());
        metaDataCopy.push_back(stateInfo);
    }

    std::vector<std::unique_ptr<SingleStateIterator>> heapPriorityQueueIterators;
    int pqStateId = static_cast<int>(metaDataCopy.size());

    if (registeredPQStates != nullptr && !registeredPQStates->empty()) {
        std::vector<std::string> pqStateNames;
        pqStateNames.reserve(registeredPQStates->size());
        for (const auto& entry : *registeredPQStates) {
            pqStateNames.push_back(entry.first);
        }
        std::sort(pqStateNames.begin(), pqStateNames.end());

        for (const auto& stateName : pqStateNames) {
            auto iter = registeredPQStates->find(stateName);
            if (iter == registeredPQStates->end() || iter->second == nullptr) {
                continue;
            }

            auto& wrapper = iter->second;
            stateMetaInfoSnapshots.push_back(wrapper->snapshotMetaInfo());

            auto pqIterator = wrapper->createSnapshotIterator(
                pqStateId,
                keyGroupPrefixBytes);
            if (pqIterator != nullptr) {
                heapPriorityQueueIterators.push_back(std::move(pqIterator));
            }

            ++pqStateId;
        }
    }

    auto lease = rocksDBResourceGuard->acquireResource();
    auto snapshot = db->GetSnapshot();
    return std::make_shared<RocksDBFullSnapshotResources>(
        lease,
        snapshot,
        metaDataCopy,
        stateMetaInfoSnapshots,
        std::move(heapPriorityQueueIterators),
        db,
        keyGroupPrefixBytes,
        keyGroupRange,
        keySerializer);
}

RocksDBFullSnapshotResources::~RocksDBFullSnapshotResources() {
    if (db_ != nullptr && snapshot_ != nullptr) {
        db_->ReleaseSnapshot(snapshot_);
    }
    if (lease_ != nullptr) {
        lease_->close();
        delete lease_;
    }
}

void RocksDBFullSnapshotResources::cleanup() {
    if (db_ != nullptr && snapshot_ != nullptr) {
        db_->ReleaseSnapshot(snapshot_);
        snapshot_ = nullptr;
    }
    if (lease_ != nullptr) {
        lease_->close();
        delete lease_;
        lease_ = nullptr;
    }
}