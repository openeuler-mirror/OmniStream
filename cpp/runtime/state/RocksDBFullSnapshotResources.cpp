#include "RocksDBFullSnapshotResources.h"
#include "state/rocksdb/iterator/SingleStateIterator.h"
#include "state/rocksdb/iterator/RocksStatesPerKeyGroupMergeIterator.h"
#include "state/rocksdb/iterator/RocksTransformingIteratorWrapper.h"
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
    std::vector<std::unique_ptr<SingleStateIterator>> heapPriorityQueueIterators;
    return std::make_shared<RocksStatesPerKeyGroupMergeIterator>(
        std::move(closeableRegistry),
        kvStateIterators,
        heapPriorityQueueIterators,
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
    keySerializer_(keySerializer)
{
    for (auto& info : metaDataCopy) {
        metaData_.push_back(std::make_shared<MetaData>(info, nullptr));
    }
}

std::shared_ptr<RocksDBFullSnapshotResources>
RocksDBFullSnapshotResources::create(
    const std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>& kvStateInformation,
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
    auto lease = rocksDBResourceGuard->acquireResource();
    auto snapshot = db->GetSnapshot();
    return std::make_shared<RocksDBFullSnapshotResources>(
        lease,
        snapshot,
        metaDataCopy,
        stateMetaInfoSnapshots,
        db,
        keyGroupPrefixBytes,
        keyGroupRange,
        keySerializer);
}