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

#include <memory>
#include "AggregateWindowOperator.h"
#include "table/runtime/operators/aggregate/handler/GroupingWindowAggsCountHandler.h"
#include "table/runtime/operators/aggregate/handler/GroupingWindowAggsCompositeHandler.h"
#include "table/runtime/operators/aggregate/handler/GroupingWindowAggsSumHandler.h"

template<typename K, typename W>
std::unique_ptr<NamespaceAggsHandleFunction<W>> AggregateWindowOperator<K, W>::initNamespaceAggsHandleFunctions(const nlohmann::json &aggInfoList) {
    // TODO: namespace is unused in functions, causing WindowOperator only supports session window(each key corresponds to only one window)
    auto aggregateCalls = aggInfoList["aggregateCalls"].get<vector<nlohmann::json>>();
    auto accTypes = aggInfoList["AccTypes"].get<vector<std::string>>();
    auto aggValueTypes = aggInfoList["aggValueTypes"].get<vector<std::string>>();
    auto indexOfCountStar = aggInfoList["indexOfCountStar"].get<int>();

    // TODO: namespace currently only supports TimeWindow
    std::vector<std::unique_ptr<NamespaceAggsHandleFunction<TimeWindow>>> functions;
    auto accStartIndex = 0;
    auto valueStartIndex = 0;
    for (int i = 0; i < aggregateCalls.size(); ++i) {
        auto aggregateCall = aggregateCalls[i];
        std::string aggTypeStr = aggregateCall["name"];
        std::string aggregationFunction = aggregateCall["aggregationFunction"];
        auto aggIndex = WindowOperator<K, W>::getSingleArgIndex(aggregateCall);
        std::string aggType = WindowOperator<K, W>::extractAggFunction(aggTypeStr);
        int filterIndex = aggregateCall["filterArg"]; // TODO: not support now
        std::string consumeRetraction = aggregateCall["consumeRetraction"]; // TODO: not support now

        // when aggIndex is -1, the agg function must be COUNT(*) ?
        auto aggDataType = aggIndex == -1 ? DataTypeId::OMNI_LONG : this->inputTypesId[aggIndex];

        // aggIndex -> column index in the input row
        // accIndex -> agg function value index in Intermediate results row (accumulators)
        // valueIndex -> agg function value index in results row (output)
        if (aggType == "COUNT") {
            auto function = std::make_unique<GroupingWindowAggsCountHandler<TimeWindow>>(
                    aggIndex, aggDataType, accStartIndex, valueStartIndex, filterIndex);
            functions.push_back(std::move(function));
        } else if (aggType == "SUM") {
            auto function = std::make_unique<GroupingWindowAggsSumHandler<TimeWindow>>(
                    aggIndex, aggDataType, accStartIndex, valueStartIndex, filterIndex);
            functions.push_back(std::move(function));
        } else {
            THROW_LOGIC_EXCEPTION("Unsupported aggregate type: " + aggType);
        }
        accStartIndex++;
        valueStartIndex++;
    }
    const auto &fullOutputTypeIds = WindowOperator<K, W>::outputTypesId;
    const auto keyArity = WindowOperator<K, W>::keyedIndex.size();
    if (keyArity > fullOutputTypeIds.size()) {
        THROW_LOGIC_EXCEPTION("The size of key fields must not exceed output type fields.");
    }
    std::vector<int32_t> valueOutputTypeIds(fullOutputTypeIds.begin() + keyArity, fullOutputTypeIds.end());
    return std::make_unique<GroupingWindowAggsCompositeHandler<TimeWindow>>(
            std::move(functions),
            this->windowPropertyTypesId,
            std::move(valueOutputTypeIds));
}

template class AggregateWindowOperator<RowData*, TimeWindow>;
