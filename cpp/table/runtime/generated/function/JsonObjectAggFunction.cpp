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
#include "JsonObjectAggFunction.h"
#include <algorithm>
#include <optional>
#include <vector>
#include "JsonAggUtil.h"
#include "runtime/dataview/PerKeyStateDataViewStore.h"
#include "core/typeutils/StringSerializer.h"

namespace {
// getValue output buffer kept alive across the setStringView call.
thread_local std::string g_jsonObjectAggOutput;
} // namespace

bool JsonObjectAggFunction::equaliser(BinaryRowData* r1, BinaryRowData* r2)
{
    // No value-based state short-circuit (same conservative default as LastStringValueFunction).
    return false;
}

void JsonObjectAggFunction::open(StateDataViewStore* store)
{
    this->store = store;
    if (keyTypeId != DataTypeId::OMNI_VARCHAR) {
        throw std::runtime_error("JSON_OBJECTAGG key must be a string type.");
    }
    auto* perKeyViewStore = reinterpret_cast<PerKeyStateDataViewStore<RowData*>*>(store);
    mapView = reinterpret_cast<KeyedStateMapViewWithKeysNotNull<VoidNamespace, std::string, std::string>*>(
        perKeyViewStore->getStateMapView<VoidNamespace, std::string, std::string>(
            "json_object_agg_" + std::to_string(aggFuncIndex),
            false,
            new StringSerializer(),
            new StringSerializer()));
}

void JsonObjectAggFunction::putPair(const std::string& key, bool valueIsNullInput, const std::string& valueJson)
{
    // ABSENT ON NULL: drop the pair entirely when the value is NULL.
    if (valueIsNullInput && onNullAbsent) {
        return;
    }
    if (mapView->contains(std::optional<std::string>{key})) {
        throw std::runtime_error("Duplicate key in JSON_OBJECTAGG: " + key);
    }
    // NULL ON NULL: store JSON null literal; otherwise store the pre-serialized value JSON text.
    const std::string stored = valueIsNullInput ? std::string("null") : valueJson;
    mapView->put(std::optional<std::string>{key}, stored);
    entryCount++;
    valueIsNull = false;
}

void JsonObjectAggFunction::accumulate(RowData* accInput)
{
    if (accInput->isNullAt(keyIdx)) {
        throw std::runtime_error("JSON_OBJECTAGG key must not be null.");
    }
    std::string key = std::string(accInput->getStringView(keyIdx));
    const bool valNull = accInput->isNullAt(valueIdx);
    // The planner (WrapJsonAggFunctionArgumentsRule) always wraps the VALUE argument with
    // JSON_STRING, so the value column is already a serialized JSON fragment (e.g. "\"v\"", "1").
    // Store it verbatim; getValue concatenates raw fragments (matches Flink JsonObjectAggFunction).
    std::string valueJson;
    if (!valNull) {
        valueJson = std::string(accInput->getStringView(valueIdx));
    }
    putPair(key, valNull, valueJson);
}

void JsonObjectAggFunction::accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    auto* keyCol = input->Get(keyIdx);
    auto* valCol = input->Get(valueIdx);
    for (int rowIndex : indices) {
        if (keyCol->IsNull(rowIndex)) {
            throw std::runtime_error("JSON_OBJECTAGG key must not be null.");
        }
        std::string key = std::string(omnistream::jsonagg::ReadStringFromColumn(keyCol, rowIndex));
        const bool valNull = valCol->IsNull(rowIndex);
        // Value column is the JSON_STRING output (already-serialized JSON text); store verbatim.
        std::string valueJson;
        if (!valNull) {
            valueJson = std::string(omnistream::jsonagg::ReadStringFromColumn(valCol, rowIndex));
        }
        putPair(key, valNull, valueJson);
    }
}

void JsonObjectAggFunction::retract(RowData* retractInput)
{
    throw std::runtime_error("JSON_OBJECTAGG does not support retract; plan should fall back to Java.");
}

void JsonObjectAggFunction::retract(omnistream::VectorBatch* input, const std::vector<int>& indices)
{
    throw std::runtime_error("JSON_OBJECTAGG does not support retract; plan should fall back to Java.");
}

void JsonObjectAggFunction::merge(RowData* otherAcc)
{
    throw std::runtime_error("JSON_OBJECTAGG does not support merge; plan should fall back to Java.");
}

void JsonObjectAggFunction::setAccumulators(RowData* acc)
{
    // No BinaryRowData accumulator slot: all state lives in the keyed MapView, which is already
    // scoped to the current key by the framework. Nothing to load from `acc` (arity 0).
    entryCount = 0L;
    valueIsNull = false;
}

void JsonObjectAggFunction::resetAccumulators()
{
    entryCount = 0L;
    valueIsNull = false;
    if (mapView == nullptr) {
        return;
    }
    auto* entries = mapView->entries();
    if (entries != nullptr) {
        std::vector<std::string> keysToRemove;
        keysToRemove.reserve(entries->size());
        for (const auto& entry : *entries) {
            keysToRemove.push_back(entry.first);
        }
        for (const auto& key : keysToRemove) {
            mapView->remove(std::optional<std::string>{key});
        }
    }
}

void JsonObjectAggFunction::getAccumulators(BinaryRowData* accumulators)
{
    // No BinaryRowData accumulator slot (arity 0); state is persisted in the keyed MapView on put().
}

void JsonObjectAggFunction::createAccumulators(BinaryRowData* accumulators)
{
    // No BinaryRowData accumulator slot (arity 0); nothing to initialize here.
}

void JsonObjectAggFunction::getValue(BinaryRowData* aggValue)
{
    if (valueIndex < 0) {
        return;
    }
    std::vector<std::pair<std::string, std::string>> kv;
    if (mapView != nullptr) {
        auto* entries = mapView->entries();
        if (entries != nullptr) {
            kv.reserve(entries->size());
            for (const auto& entry : *entries) {
                kv.emplace_back(entry.first, entry.second);
            }
        }
    }
    // Deterministic output: emit keys in ascending order (matches documented examples).
    std::sort(kv.begin(), kv.end(), [](const auto& a, const auto& b) { return a.first < b.first; });

    g_jsonObjectAggOutput.clear();
    g_jsonObjectAggOutput.push_back('{');
    for (size_t i = 0; i < kv.size(); ++i) {
        if (i != 0) {
            g_jsonObjectAggOutput.push_back(',');
        }
        omnistream::jsonagg::AppendJsonEscapedString(g_jsonObjectAggOutput, kv[i].first);
        g_jsonObjectAggOutput.push_back(':');
        g_jsonObjectAggOutput += kv[i].second;
    }
    g_jsonObjectAggOutput.push_back('}');
    aggValue->setStringView(valueIndex, std::string_view(g_jsonObjectAggOutput));
}
