/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#include <iterator>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include "CountDistinctFunction.h"
#include "typeutils/InternalSerializers.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"

CountDistinctFunction::DistinctCacheIter CountDistinctFunction::findOrLoadDistinctCache(RowData* groupKey)
{
    if (groupKey == nullptr) {
        return distinctCacheLru.end();
    }

    auto cacheIt = distinctCacheIndex.find(groupKey);
    if (cacheIt != distinctCacheIndex.end()) {
        touchDistinctCache(cacheIt->second);
        return cacheIt->second;
    }

    RowData* keyCopy = groupKey->copy();
    if (keyCopy == nullptr) {
        return distinctCacheLru.end();
    }

    distinctCacheLru.push_front(DistinctCacheNode{keyCopy, {}});
    auto nodeIt = distinctCacheLru.begin();
    nodeIt->distinctKeys.reserve(64);
    nodeIt->distinctKeyOrder.clear();

    distinctCacheIndex.emplace(nodeIt->groupKey, nodeIt);
    evictDistinctCacheIfNeeded();
    return nodeIt;
}

void CountDistinctFunction::touchDistinctCache(DistinctCacheIter it)
{
    if (it != distinctCacheLru.begin()) {
        distinctCacheLru.splice(distinctCacheLru.begin(), distinctCacheLru, it);
    }
}

void CountDistinctFunction::evictDistinctCacheIfNeeded()
{
    while (distinctCacheLru.size() > DISTINCT_CACHE_CAPACITY) {
        auto tailIt = std::prev(distinctCacheLru.end());
        RowData* staleKey = tailIt->groupKey;
        distinctCacheIndex.erase(staleKey);
        delete staleKey;
        distinctCacheLru.pop_back();
    }
}

void CountDistinctFunction::invalidateDistinctCache(RowData* groupKey)
{
    if (groupKey == nullptr) {
        return;
    }
    auto cacheIt = distinctCacheIndex.find(groupKey);
    if (cacheIt == distinctCacheIndex.end()) {
        return;
    }

    auto nodeIt = cacheIt->second;
    RowData* cachedKey = nodeIt->groupKey;
    distinctCacheLru.erase(nodeIt);
    distinctCacheIndex.erase(cacheIt);
    delete cachedKey;
}

void CountDistinctFunction::clearDistinctCache()
{
    for (auto& node : distinctCacheLru) {
        delete node.groupKey;
    }
    distinctCacheLru.clear();
    distinctCacheIndex.clear();
}

bool CountDistinctFunction::equaliser(BinaryRowData *r1, BinaryRowData *r2)
{
    if (r1->isNullAt(valueIndex) || r2->isNullAt(valueIndex)) {
        return false;
    }
    bool isEqual = false;
    switch (typeId) {
        case DataTypeId::OMNI_INT: {
            isEqual = *r1->getInt(valueIndex) == *r2->getInt(valueIndex);
            break;
        }
        case DataTypeId::OMNI_LONG: {
            isEqual = *r1->getLong(valueIndex) == *r2->getLong(valueIndex);
            break;
        }
        default:
            LOG("Data type is not supported.");
            throw std::runtime_error("Data type is not supported.");
    }
    return isEqual;
}

void CountDistinctFunction::open(StateDataViewStore *store)
{
    this->store = store;
    distinctCacheIndex.reserve(DISTINCT_CACHE_CAPACITY);
    pendingDistinctUpdates.reserve(64);
    auto *perKeyViewStore =
        reinterpret_cast<PerKeyStateDataViewStore<RowData *> *>(store);
    // todo support more data types
    switch (typeId) {
        case DataTypeId::OMNI_INT:
        case DataTypeId::OMNI_LONG: {
            distinctMapView = reinterpret_cast<KeyedStateMapViewWithKeysNullable<VoidNamespace, long, long> *>(
                    perKeyViewStore->getStateMapView<VoidNamespace, long, long>("distinct_acc" + std::to_string(aggFuncIndex), true,
                                                                                InternalSerializers::create(new BasicLogicalType(typeId, false)),
                                                                                InternalSerializers::create(new BasicLogicalType(typeId, false))));

            break;
        }
        default:
            LOG("Data type is not supported.");
            throw std::runtime_error("Data type is not supported.");
    }
}


void CountDistinctFunction::accumulate(RowData *accInput)
{
    bool isFieldNull = accInput->isNullAt(aggIdx);
    long fieldValue = 0L;
    switch (typeId) {
        case DataTypeId::OMNI_INT: {
            fieldValue = isFieldNull ? -1L : *accInput->getInt(aggIdx);
            break;
        }
        case DataTypeId::OMNI_LONG: {
            fieldValue = isFieldNull ? -1L : *accInput->getLong(aggIdx);
            break;
        }
        default:
            LOG("Data type is not supported.");
            throw std::runtime_error("Data type is not supported.");
    }
    bool shouldDoAccumulate = !hasFilter || (hasFilter && *accInput->getBool(filterIndex));
    if (shouldDoAccumulate && !isFieldNull) {
        std::optional<long> value = distinctMapView->get(std::optional<long> { fieldValue });
        if (!value.has_value()) {
            if (!valueIsNull) {
                aggCount++;
            } else {
                aggCount = 1L;
                valueIsNull = false;
            }
            distinctMapView->put(std::optional<long> { fieldValue }, 0L);
        }
    }

    LOG("Accumulate. Count:  " << aggCount << " countIsNull: " << valueIsNull)
}

void CountDistinctFunction::accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    if (backend == 2) {
        this->accumulateInRocksDB(input, indices);
    } else {
        auto columnData = input->Get(aggIdx);
        const bool hasFilterCol = hasFilter;
        const auto filterData = hasFilterCol
                                    ? reinterpret_cast<omniruntime::vec::Vector<bool>*>(input->Get(filterIndex))
                                    : nullptr;
        for (int rowIndex : indices) {
            bool shouldDoAccumulate = true;
            if (hasFilterCol) {
                bool isFilterNull = filterData->IsNull(rowIndex);
                shouldDoAccumulate = !isFilterNull && filterData->GetValue(rowIndex);
            }
            if (!shouldDoAccumulate) continue;
            bool isFieldNull = columnData->IsNull(rowIndex);
            if (isFieldNull) {
                continue;
            }

            long fieldValue = 0L;
            switch (typeId) {
            case DataTypeId::OMNI_INT: {
                fieldValue = dynamic_cast<omniruntime::vec::Vector<int>*>(columnData)->GetValue(rowIndex);
                break;
            }
            case DataTypeId::OMNI_LONG: {
                fieldValue = dynamic_cast<omniruntime::vec::Vector<long>*>(columnData)->GetValue(rowIndex);
                break;
            }
            default:
                LOG("Data type is not supported.");
                throw std::runtime_error("Data type is not supported.");
            }
            std::optional<long> value = distinctMapView->get(std::optional<long>{fieldValue});
            if (!value.has_value()) {
                if (!valueIsNull) {
                    aggCount++;
                }
                else {
                    aggCount = 1L;
                    valueIsNull = false;
                }
                distinctMapView->put(std::optional<long>{fieldValue}, 0L);
            }
        }
        LOG("Accumulate. Count: " << aggCount << " valueIsNull: " << valueIsNull);
    }
}


void CountDistinctFunction::accumulateInRocksDB(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    auto cacheNode = findOrLoadDistinctCache(currentGroupKey);
    auto pendingGroupUpdatesIt = pendingDistinctUpdates.end();
    if (currentGroupKey != nullptr) {
        auto [it, inserted] = pendingDistinctUpdates.try_emplace(currentGroupKey);
        if (inserted) {
            it->second.reserve(64);
        }
        pendingGroupUpdatesIt = it;
    }

    auto columnData = input->Get(aggIdx);
    const bool hasFilterCol = hasFilter;
    const auto filterData = hasFilterCol
                                ? reinterpret_cast<omniruntime::vec::Vector<bool>*>(input->Get(filterIndex))
                                : nullptr;
    auto* intColumn = (typeId == DataTypeId::OMNI_INT)
                          ? dynamic_cast<omniruntime::vec::Vector<int>*>(columnData)
                          : nullptr;
    auto* longColumn = (typeId == DataTypeId::OMNI_LONG)
                           ? dynamic_cast<omniruntime::vec::Vector<long>*>(columnData)
                           : nullptr;

    if ((typeId == DataTypeId::OMNI_INT && intColumn == nullptr) ||
        (typeId == DataTypeId::OMNI_LONG && longColumn == nullptr)) {
        LOG("Input column type mismatch for COUNT DISTINCT.");
        throw std::runtime_error("Input column type mismatch for COUNT DISTINCT.");
    }

    // Stage 1: deduplicate incoming records within this batch.
    std::unordered_set<long> batchDistinctKeys;
    batchDistinctKeys.reserve(indices.size());

    for (int rowIndex : indices) {
        bool shouldDoAccumulate = true;
        if (hasFilterCol) {
            bool isFilterNull = filterData->IsNull(rowIndex);
            shouldDoAccumulate = !isFilterNull && filterData->GetValue(rowIndex);
        }
        if (!shouldDoAccumulate) {
            continue;
        }

        if (columnData->IsNull(rowIndex)) {
            continue;
        }

        long fieldValue = 0L;
        switch (typeId) {
            case DataTypeId::OMNI_INT: {
                fieldValue = static_cast<long>(intColumn->GetValue(rowIndex));
                break;
            }
            case DataTypeId::OMNI_LONG: {
                fieldValue = longColumn->GetValue(rowIndex);
                break;
            }
            default:
                LOG("Data type is not supported.");
                throw std::runtime_error("Data type is not supported.");
        }

        batchDistinctKeys.emplace(fieldValue);
    }

    // Stage 2: compare deduplicated keys with cache + state and collect new keys for deferred batch flush.
    for (const long fieldValue : batchDistinctKeys) {
        if (cacheNode != distinctCacheLru.end()) {
            auto& cachedDistinctSet = cacheNode->distinctKeys;
            auto& distinctKeyOrder = cacheNode->distinctKeyOrder;
            auto cacheIt = cachedDistinctSet.find(fieldValue);
            if (cacheIt != cachedDistinctSet.end()) {
                continue;
            }

            cachedDistinctSet.emplace(fieldValue);
            distinctKeyOrder.push_back(fieldValue);
            while (cachedDistinctSet.size() > DISTINCT_KEYS_PER_GROUP_CAPACITY) {
                const long evictedKey = distinctKeyOrder.front();
                distinctKeyOrder.pop_front();
                cachedDistinctSet.erase(evictedKey);
            }
        }

        if (pendingGroupUpdatesIt != pendingDistinctUpdates.end()) {
            auto& pendingGroupUpdates = pendingGroupUpdatesIt->second;
            if (pendingGroupUpdates.find(fieldValue) != pendingGroupUpdates.end()) {
                continue;
            }
        }

        std::optional<long> stateValue = distinctMapView->get(std::optional<long>{fieldValue});
        if (stateValue.has_value()) {
            continue;
        }

        if (pendingGroupUpdatesIt != pendingDistinctUpdates.end()) {
            pendingGroupUpdatesIt->second.emplace(fieldValue, 0L);
        }

        if (!valueIsNull) {
            aggCount++;
        } else {
            aggCount = 1L;
            valueIsNull = false;
        }
    }

    LOG("Accumulate. Count: " << aggCount << " valueIsNull: " << valueIsNull);
}

void CountDistinctFunction::retract(RowData *retractInput)
{
}

void CountDistinctFunction::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
}

void CountDistinctFunction::merge(RowData *otherAcc)
{
}


void CountDistinctFunction::setAccumulators(RowData *acc)
{
    valueIsNull = acc->isNullAt(accIndex);
    aggCount = valueIsNull ? -1L : *acc->getLong(accIndex);
    LOG("Set Acc. Count:  " << aggCount << " countIsNull: " << valueIndex)
}


void CountDistinctFunction::resetAccumulators()
{
    aggCount = (static_cast<long>(0L));
    valueIsNull = false;

    pendingDistinctUpdates.erase(currentGroupKey);

    auto* entries = distinctMapView->entries();
    if (entries != nullptr) {
        std::vector<long> keysToRemove;
        keysToRemove.reserve(entries->size());
        for (const auto& entry : *entries) {
            keysToRemove.push_back(entry.first);
        }
        for (const long distinctKey : keysToRemove) {
            distinctMapView->remove(std::optional<long>{distinctKey});
        }
    }

    invalidateDistinctCache(currentGroupKey);
    LOG("Reset Acc. Count:  " << aggCount << " countIsNull: " << valueIsNull)
}


void CountDistinctFunction::getAccumulators(BinaryRowData *accumulators)
{
    if (valueIsNull) {
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, aggCount);
    }
    LOG("Get acc: " << aggCount)
}


void CountDistinctFunction::createAccumulators(BinaryRowData *accumulators)
{
    if (false) { // This condition is always false, but it's in the original code.
        accumulators->setNullAt(accIndex);
    } else {
        accumulators->setLong(accIndex, 0L);
    }
    LOG("Create acc")
}


void CountDistinctFunction::getValue(BinaryRowData *newAggValue)
{
    if (valueIsNull) {
        newAggValue->setNullAt(valueIndex);
    } else {
        newAggValue->setLong(valueIndex, aggCount);
    }
    LOG("Get value: " << aggCount)
}

void CountDistinctFunction::setCurrentGroupKey(RowData* key)
{
    this->currentGroupKey = key;
}

void CountDistinctFunction::updateInnerState()
{
    size_t pendingSize = 0;
    for (const auto& groupEntry : pendingDistinctUpdates) {
        pendingSize += groupEntry.second.size();
    }

    std::vector<std::shared_ptr<std::tuple<RowData*, long, long>>> pendingTupleUpdates;
    pendingTupleUpdates.reserve(pendingSize);
    for (auto& groupEntry : pendingDistinctUpdates) {
        RowData* groupKey = groupEntry.first;
        if (groupKey == nullptr || groupEntry.second.empty()) {
            continue;
        }
        for (const auto& keyValue : groupEntry.second) {
            pendingTupleUpdates.push_back(
                std::make_shared<std::tuple<RowData*, long, long>>(
                    std::make_tuple(groupKey, keyValue.first, keyValue.second)));
        }
    }

    if (!pendingTupleUpdates.empty()) {
        distinctMapView->putByBatch(pendingTupleUpdates);
    }
    pendingDistinctUpdates.clear();
    this->distinctMapView->cleanup();
}

void CountDistinctFunction::cleanup()
{
    pendingDistinctUpdates.clear();
}

void CountDistinctFunction::close()
{
    pendingDistinctUpdates.clear();
    clearDistinctCache();
}

CountDistinctFunction::~CountDistinctFunction()
{
    clearDistinctCache();
}
