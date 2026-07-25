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
 * Description: Native handler for Flink SQL JSON_ARRAYAGG(items [ {NULL|ABSENT} ON NULL ]).
 * Aggregate: builds a single JSON array string by folding per-row items across a group, preserving
 * insertion order.
 *
 * Accumulator model = Scheme B (StateDataView / MapView). Flink uses a ListView; OmniStream has no
 * StateListView, so we emulate an ordered list with an index-keyed MapView:
 *   - keyed MapView<VoidNamespace, long, std::string> : insertion-index -> serialized element JSON text
 *   - NO BinaryRowData accumulator slot is used (accumulatorSlots()==0). Flink's accumulator is a RAW
 *     MapView type that GroupAggFunction filters out of accTypes, so accumulatorArity does not count it.
 *     The next insertion index is recomputed per-key from the MapView element count in setAccumulators.
 *
 * Semantics:
 *   - element NULL: default ABSENT ON NULL -> skip; NULL ON NULL -> append JSON null.
 *   - elements are emitted in ascending index (insertion) order.
 *   - retract / merge are NOT supported (throw) -> retraction plans fall back to Java (see design doc §4.5).
 */

#ifndef FLINK_TNEL_JSONARRAYAGGFUNCTION_H
#define FLINK_TNEL_JSONARRAYAGGFUNCTION_H

#include <string>
#include "../AggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"
#include "../runtime/state/VoidNamespace.h"

using namespace omniruntime::type;

class JsonArrayAggFunction : public AggsHandleFunction {
public:
    JsonArrayAggFunction(
        int itemIdx,
        const std::string& itemType,
        int aggFuncIndex = -1,
        bool onNullAbsent = true,
        int accIndex = -1,
        int valueIndex = -1)
        : itemIdx(itemIdx),
          aggFuncIndex(aggFuncIndex),
          onNullAbsent(onNullAbsent),
          accIndex(accIndex),
          valueIndex(valueIndex),
          nextIndex(0),
          valueIsNull(true)
    {
        itemTypeId = LogicalType::flinkTypeToOmniTypeId(itemType);
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
    // Append one element to the index-keyed map view, applying ON NULL behavior.
    void putItem(bool itemIsNullInput, const std::string& itemJson);

    int itemIdx;
    int aggFuncIndex;
    bool onNullAbsent;
    int accIndex;
    int valueIndex;
    long nextIndex;
    bool valueIsNull;
    DataTypeId itemTypeId;
    StateDataViewStore* store = nullptr;
    // Index keys are never null, so use the not-null map view and avoid creating a null-key
    // ValueState<std::string> (unsupported by the heap backend's VALUE dispatch).
    KeyedStateMapViewWithKeysNotNull<VoidNamespace, long, std::string>* mapView = nullptr;
    RowData* currentGroupKey = nullptr;
};

#endif // FLINK_TNEL_JSONARRAYAGGFUNCTION_H
