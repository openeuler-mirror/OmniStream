#include "RocksDBFullSnapshotResources.h"
#include "state/RocksDBVectorBatchStateAccessor.h"
#include "state/rocksdb/iterator/SingleStateIterator.h"
#include "state/rocksdb/iterator/RocksStatesPerKeyGroupMergeIterator.h"
#include "state/rocksdb/iterator/RocksTransformingIteratorWrapper.h"
#include <algorithm>
#include "RocksDBConfigurableOptions.h"
#include "../../include/functions/Configuration.h"

namespace {
std::string resolveVectorBatchStateName(const std::string& logicalStateName)
{
    return logicalStateName + "vb";
}
} // namespace

const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& RocksDBFullSnapshotResources::getMetaInfoSnapshots()
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

    // [FALCON] -----------------------------------------------------------------------------------------------
    auto useHashMemTable =
        reinterpret_cast<Boolean*>(Configuration::TM_CONFIG->getValue(RocksDBConfigurableOptions::USE_HASH_MEMTABLE));

    if (useHashMemTable != nullptr && useHashMemTable->value) {
        readOptions.total_order_seek = true;
    }

    if (useHashMemTable != nullptr) {
        useHashMemTable->putRefCount();
    }
    // [FALCON] -----------------------------------------------------------------------------------------------

    readOptions.snapshot = snapshot_;
    std::vector<std::pair<std::unique_ptr<RocksIteratorWrapper>, int>> kvStateIterators =
        createKVStateIterators(closeableRegistry, readOptions);
    return std::make_shared<RocksStatesPerKeyGroupMergeIterator>(
        std::move(closeableRegistry), kvStateIterators, heapPriorityQueueIterators_, keyGroupPrefixBytes_);
}

bool RocksDBFullSnapshotResources::isHeapPriorityQueueStateId(int kvStateId) const
{
    return heapPriorityQueueStateIds_.find(kvStateId) != heapPriorityQueueStateIds_.end();
}

std::shared_ptr<VectorBatchStateAccessor> RocksDBFullSnapshotResources::createVectorBatchStateAccessor(
    const std::string& logicalStateName, const VectorBatchAccessorOptions& options)
{
    if (db_ == nullptr || snapshot_ == nullptr || keyGroupRange_ == nullptr || keyGroupPrefixBytes_ <= 0 ||
        keyGroupRange_->getNumberOfKeyGroups() <= 0) {
        return nullptr;
    }

    const std::string stateName = resolveVectorBatchStateName(logicalStateName);
    auto iter = metaDataByStateName_.find(stateName);
    if (iter == metaDataByStateName_.end() || iter->second == nullptr) {
        return nullptr;
    }

    const std::shared_ptr<RocksDbKvStateInfo>& kvStateInfo = iter->second->rocksDbKvStateInfo;
    if (kvStateInfo == nullptr || kvStateInfo->columnFamilyHandle_ == nullptr) {
        return nullptr;
    }

    return std::make_shared<RocksDBVectorBatchStateAccessor>(
        db_,
        kvStateInfo->columnFamilyHandle_,
        snapshot_,
        keyGroupPrefixBytes_,
        keyGroupRange_->getStartKeyGroup(),
        options);
}

std::vector<std::pair<std::unique_ptr<RocksIteratorWrapper>, int>> RocksDBFullSnapshotResources::createKVStateIterators(
    std::unique_ptr<CloseableRegistry>& closeableRegistry, const rocksdb::ReadOptions& readOptions)
{
    std::vector<std::pair<std::unique_ptr<RocksIteratorWrapper>, int>> kvStateIterators;
    int kvStateId = 0;
    for (auto& metaDataEntry : metaData_) {
        auto rocksIteratorWrapper = createRocksIteratorWrapper(
            db_,
            metaDataEntry->rocksDbKvStateInfo->columnFamilyHandle_,
            metaDataEntry->stateSnapshotTransformer.get(),
            readOptions);
        kvStateIterators.push_back(std::pair{std::move(rocksIteratorWrapper), kvStateId});
        kvStateId++;
    }
    return kvStateIterators;
}

std::unique_ptr<RocksIteratorWrapper> RocksDBFullSnapshotResources::createRocksIteratorWrapper(
    rocksdb::DB* db,
    rocksdb::ColumnFamilyHandle* columnFamilyHandle,
    StateSnapshotTransformer<std::vector<int8_t>>* stateSnapshotTransformer,
    const rocksdb::ReadOptions& readOptions)
{
    std::unique_ptr<rocksdb::Iterator> rocksIterator;
    rocksIterator.reset(db->NewIterator(readOptions, columnFamilyHandle));
    if (stateSnapshotTransformer == nullptr) {
        return std::make_unique<RocksIteratorWrapper>(std::move(rocksIterator));
    } else {
        return std::make_unique<RocksTransformingIteratorWrapper>(
            std::move(rocksIterator), std::move(stateSnapshotTransformer));
    }
}

RocksDBFullSnapshotResources::RocksDBFullSnapshotResources(
    ResourceGuard::Lease* lease,
    const rocksdb::Snapshot* snapshot,
    const std::vector<std::shared_ptr<RocksDbKvStateInfo>>& metaDataCopy,
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& stateMetaInfoSnapshots,
    std::vector<std::unique_ptr<SingleStateIterator>> heapPriorityQueueIterators,
    std::unordered_set<int> heapPriorityQueueStateIds,
    rocksdb::DB* db,
    int keyGroupPrefixBytes,
    KeyGroupRange* keyGroupRange,
    TypeSerializer* keySerializer)
    : lease_(lease),
      snapshot_(snapshot),
      stateMetaInfoSnapshots_(stateMetaInfoSnapshots),
      db_(db),
      keyGroupPrefixBytes_(keyGroupPrefixBytes),
      keyGroupRange_(keyGroupRange),
      keySerializer_(keySerializer),
      heapPriorityQueueIterators_(std::move(heapPriorityQueueIterators)),
      heapPriorityQueueStateIds_(std::move(heapPriorityQueueStateIds))
{
    for (auto& info : metaDataCopy) {
        auto metaData = std::make_shared<MetaData>(info, nullptr);
        metaData_.push_back(metaData);
        if (info != nullptr && info->metaInfo_ != nullptr) {
            metaDataByStateName_[info->metaInfo_->getName()] = metaData;
        }
    }
}

std::shared_ptr<RocksDBFullSnapshotResources> RocksDBFullSnapshotResources::create(
    const std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>& kvStateInformation,
    const std::shared_ptr<
        std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>&
        registeredPQStates,
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
    std::unordered_set<int> heapPriorityQueueStateIds;
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

            auto pqIterator = wrapper->createSnapshotIterator(pqStateId, keyGroupPrefixBytes);
            if (pqIterator != nullptr) {
                heapPriorityQueueIterators.push_back(std::move(pqIterator));
            }
            heapPriorityQueueStateIds.insert(pqStateId);

            ++pqStateId;
        }
    }

    ResourceGuard::Lease* lease = nullptr;
    const rocksdb::Snapshot* snapshot = nullptr;
    if (db != nullptr && rocksDBResourceGuard != nullptr) {
        lease = rocksDBResourceGuard->acquireResource();
        snapshot = db->GetSnapshot();
    }
    return std::make_shared<RocksDBFullSnapshotResources>(
        lease,
        snapshot,
        metaDataCopy,
        stateMetaInfoSnapshots,
        std::move(heapPriorityQueueIterators),
        std::move(heapPriorityQueueStateIds),
        db,
        keyGroupPrefixBytes,
        keyGroupRange,
        keySerializer);
}

RocksDBFullSnapshotResources::~RocksDBFullSnapshotResources()
{
    if (db_ != nullptr && snapshot_ != nullptr) {
        db_->ReleaseSnapshot(snapshot_);
    }
    if (lease_ != nullptr) {
        lease_->close();
        delete lease_;
    }
}

void RocksDBFullSnapshotResources::cleanup()
{
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
