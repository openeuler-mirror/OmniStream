/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#include "JsonArrayAggFunction.h"
#include <algorithm>
#include <optional>
#include <vector>
#include "JsonAggUtil.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"
#include "core/typeutils/LongSerializer.h"
#include "core/typeutils/StringSerializer.h"

namespace {
// getValue output buffer kept alive across the setStringView call.
thread_local std::string g_jsonArrayAggOutput;
} // namespace

bool JsonArrayAggFunction::equaliser(BinaryRowData* r1, BinaryRowData* r2)
{
    return false;
}

void JsonArrayAggFunction::open(StateDataViewStore* store)
{
    this->store = store;
    auto* perKeyViewStore = reinterpret_cast<PerKeyStateDataViewStore<RowData*>*>(store);
    mapView = reinterpret_cast<KeyedStateMapViewWithKeysNotNull<VoidNamespace, long, std::string>*>(
        perKeyViewStore->getStateMapView<VoidNamespace, long, std::string>(
            "json_array_agg_" + std::to_string(aggFuncIndex),
            false,
            new LongSerializer(),
            new StringSerializer()));
}

void JsonArrayAggFunction::putItem(bool itemIsNullInput, const std::string& itemJson)
{
    // ABSENT ON NULL (array default): drop NULL elements.
    if (itemIsNullInput && onNullAbsent) {
        return;
    }
    const std::string stored = itemIsNullInput ? std::string("null") : itemJson;
    mapView->put(std::optional<long>{nextIndex}, stored);
    nextIndex++;
    valueIsNull = false;
}

void JsonArrayAggFunction::accumulate(RowData* accInput)
{
    if (hasFilter) {
        bool isFilterNull = accInput->isNullAt(filterIndex);
        bool shouldDoAccumulate = !isFilterNull && *(accInput->getBool(filterIndex));
        if (!shouldDoAccumulate) {
            return;
        }
    }
    const bool itemNull = accInput->isNullAt(itemIdx);
    // The planner (WrapJsonAggFunctionArgumentsRule) always wraps the item argument with
    // JSON_STRING, so the item column is already a serialized JSON fragment. Store it verbatim;
    // getValue concatenates raw fragments (matches Flink JsonArrayAggFunction).
    std::string itemJson;
    if (!itemNull) {
        itemJson = std::string(accInput->getStringView(itemIdx));
    }
    putItem(itemNull, itemJson);
}

void JsonArrayAggFunction::accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    auto* itemCol = input->Get(itemIdx);
    const auto filterData =
        hasFilter ? reinterpret_cast<omniruntime::vec::Vector<bool>*>(input->Get(filterIndex)) : nullptr;
    for (int rowIndex : indices) {
        if (hasFilter) {
            bool isFilterNull = filterData->IsNull(rowIndex);
            bool shouldDoAccumulate = !isFilterNull && filterData->GetValue(rowIndex);
            if (!shouldDoAccumulate) {
                continue;
            }
        }
        const bool itemNull = itemCol->IsNull(rowIndex);
        // Item column is the JSON_STRING output (already-serialized JSON text); store verbatim.
        std::string itemJson;
        if (!itemNull) {
            itemJson = std::string(omnistream::jsonagg::ReadStringFromColumn(itemCol, rowIndex));
        }
        putItem(itemNull, itemJson);
    }
}

void JsonArrayAggFunction::retract(RowData* retractInput)
{
    throw std::runtime_error("JSON_ARRAYAGG does not support retract; plan should fall back to Java.");
}

void JsonArrayAggFunction::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    throw std::runtime_error("JSON_ARRAYAGG does not support retract; plan should fall back to Java.");
}

void JsonArrayAggFunction::merge(RowData* otherAcc)
{
    throw std::runtime_error("JSON_ARRAYAGG does not support merge; plan should fall back to Java.");
}

void JsonArrayAggFunction::setAccumulators(RowData* acc)
{
    // No BinaryRowData accumulator slot: state lives in the keyed MapView (already scoped to the
    // current key). The next insertion index must be restored per-key from the current element
    // count, so subsequent appends continue after the persisted items instead of overwriting them.
    valueIsNull = false;
    nextIndex = 0L;
    if (mapView != nullptr) {
        auto* entries = mapView->entries();
        if (entries != nullptr) {
            nextIndex = static_cast<long>(entries->size());
        }
    }
}

void JsonArrayAggFunction::resetAccumulators()
{
    nextIndex = 0L;
    valueIsNull = false;
    if (mapView == nullptr) {
        return;
    }
    auto* entries = mapView->entries();
    if (entries != nullptr) {
        std::vector<long> keysToRemove;
        keysToRemove.reserve(entries->size());
        for (const auto& entry : *entries) {
            keysToRemove.push_back(entry.first);
        }
        for (const long key : keysToRemove) {
            mapView->remove(std::optional<long>{key});
        }
    }
}

void JsonArrayAggFunction::getAccumulators(BinaryRowData* accumulators)
{
    // No BinaryRowData accumulator slot (arity 0); state is persisted in the keyed MapView on put().
}

void JsonArrayAggFunction::createAccumulators(BinaryRowData* accumulators)
{
    // No BinaryRowData accumulator slot (arity 0); nothing to initialize here.
}

void JsonArrayAggFunction::getValue(BinaryRowData* aggValue)
{
    if (valueIndex < 0) {
        return;
    }
    std::vector<std::pair<long, std::string>> items;
    if (mapView != nullptr) {
        auto* entries = mapView->entries();
        if (entries != nullptr) {
            items.reserve(entries->size());
            for (const auto& entry : *entries) {
                items.emplace_back(entry.first, entry.second);
            }
        }
    }
    // Emit in ascending index (insertion) order.
    std::sort(items.begin(), items.end(), [](const auto& a, const auto& b) { return a.first < b.first; });

    g_jsonArrayAggOutput.clear();
    g_jsonArrayAggOutput.push_back('[');
    for (size_t i = 0; i < items.size(); ++i) {
        if (i != 0) {
            g_jsonArrayAggOutput.push_back(',');
        }
        g_jsonArrayAggOutput += items[i].second;
    }
    g_jsonArrayAggOutput.push_back(']');
    aggValue->setStringView(valueIndex, std::string_view(g_jsonArrayAggOutput));
}
