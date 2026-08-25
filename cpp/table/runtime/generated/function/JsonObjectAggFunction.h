/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * Description: Native handler for Flink SQL JSON_OBJECTAGG(KEY key VALUE value [ {NULL|ABSENT} ON NULL ]).
 * Aggregate: builds a single JSON object string by folding per-row key->value pairs across a group.
 *
 * Accumulator model = Scheme B (StateDataView / MapView), aligned with Flink's JsonObjectAggFunction
 * which keeps a MapView<String,String> in the accumulator:
 *   - keyed MapView<VoidNamespace, std::string, std::string> : key -> serialized value JSON text
 *   - NO BinaryRowData accumulator slot is used (accumulatorSlots()==0). Flink's accumulator is a RAW
 *     MapView type that GroupAggFunction filters out of accTypes, so accumulatorArity does not count it.
 *     Group non-emptiness is tracked by the framework's key/acc presence (AccumulationRecordCounter),
 *     not by a scalar slot; all pair state lives in the keyed MapView.
 *
 * Semantics:
 *   - FILTER (WHERE ...): filterIndex>=0 skips rows whose boolean filter column is NULL/false
 *     (same as SUM/COUNT). Checked before null-key / duplicate-key validation.
 *   - key must be a non-null string; NULL/empty key -> throw.
 *   - duplicate key within the group -> throw (Flink: keys must be unique).
 *   - value NULL: NULL ON NULL (default) -> store JSON null; ABSENT ON NULL -> skip the key.
 *   - output keys are emitted in ascending (sorted) order for deterministic golden comparison.
 *   - retract / merge are NOT supported (throw) -> retraction plans fall back to Java (see design doc §4.5).
 */

#ifndef FLINK_TNEL_JSONOBJECTAGGFUNCTION_H
#define FLINK_TNEL_JSONOBJECTAGGFUNCTION_H

#include <string>
#include "../AggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"
#include "../runtime/state/VoidNamespace.h"

using namespace omniruntime::type;

class JsonObjectAggFunction : public AggsHandleFunction {
public:
    JsonObjectAggFunction(
        int keyIdx,
        int valueIdx,
        const std::string& keyType,
        const std::string& valueType,
        int aggFuncIndex = -1,
        bool onNullAbsent = false,
        int filterIndex = -1,
        int accIndex = -1,
        int valueIndex = -1)
        : keyIdx(keyIdx),
          valueIdx(valueIdx),
          aggFuncIndex(aggFuncIndex),
          onNullAbsent(onNullAbsent),
          filterIndex(filterIndex),
          hasFilter(filterIndex != -1),
          accIndex(accIndex),
          valueIndex(valueIndex),
          entryCount(0),
          valueIsNull(true)
    {
        keyTypeId = LogicalType::flinkTypeToOmniTypeId(keyType);
        valueTypeId = LogicalType::flinkTypeToOmniTypeId(valueType);
    }

    void setWindowSize(int windowSize) override {};
    bool equaliser(BinaryRowData* r1, BinaryRowData* r2) override;
    void open(StateDataViewStore* store);
    void accumulate(RowData* accInput) override;
    void accumulate(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void retract(RowData* retractInput) override;
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void merge(RowData* otherAcc) override;
    void setAccumulators(RowData* acc) override;
    void resetAccumulators() override;
    void getAccumulators(BinaryRowData* accumulators) override;
    void createAccumulators(BinaryRowData* accumulators) override;
    void getValue(BinaryRowData* aggValue) override;
    void cleanup() override {};
    void close() override {};
    void setCurrentGroupKey(RowData* key) override
    {
        currentGroupKey = key;
    }
    void bindAccValueIndex(int accStartIndex, int valueStartIndex) override
    {
        accIndex = accStartIndex;
        valueIndex = valueStartIndex;
    }
    // State lives entirely in the keyed MapView; no BinaryRowData accumulator slot is used
    // (the RAW MapView accType is filtered out of accumulatorArity by GroupAggFunction).
    int accumulatorSlots() const override
    {
        return 0;
    }
    bool hasAggOutput() const override
    {
        return valueIndex >= 0;
    }

private:
    // Insert one (key,value) pair into the map view, applying ON NULL + duplicate-key + null-key checks.
    // valueJsonOrNull: pre-serialized JSON text of the value, or empty when the value is SQL NULL.
    void putPair(const std::string& key, bool valueIsNullInput, const std::string& valueJson);

    int keyIdx;
    int valueIdx;
    int aggFuncIndex;
    bool onNullAbsent;
    int filterIndex;
    bool hasFilter;
    int accIndex;
    int valueIndex;
    long entryCount;
    bool valueIsNull;
    DataTypeId keyTypeId;
    DataTypeId valueTypeId;
    StateDataViewStore* store = nullptr;
    // Keys are never null (NULL key -> throw), so use the not-null map view and avoid creating a
    // null-key ValueState<std::string> (which the heap backend's VALUE dispatch does not support).
    KeyedStateMapViewWithKeysNotNull<VoidNamespace, std::string, std::string>* mapView = nullptr;
    RowData* currentGroupKey = nullptr;
};

#endif // FLINK_TNEL_JSONOBJECTAGGFUNCTION_H
