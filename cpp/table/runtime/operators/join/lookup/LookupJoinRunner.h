/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by xichen on 2025/7/19.
//

#ifndef FLINK_TNEL_LOOKUPJOINRUNNER_H
#define FLINK_TNEL_LOOKUPJOINRUNNER_H

#include <nlohmann/json.hpp>
#include "table/vectorbatch/VectorBatch.h"
#include "streaming/api/functions/ProcessFunction.h"
#include "streaming/api/functions/GeneratedCsvLookupFunction.h"
#include "table/runtime/collector/TableFunctionCollector.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "core/operators/TimestampedCollector.h"
// The original Flink has ProcessFunction as a template class with <IN, OUT>
// here both IN and OUT are VectorBatch*
class LookupJoinRunner : public ProcessFunction {
public:
    explicit LookupJoinRunner(nlohmann::json description, Collector* innerCollector);

    void open(const Configuration& parameters) override;
    void processBatch(omnistream::VectorBatch* value, Context *cxt, Collector *collector) override;

    [[nodiscard]] Collector* getFetcherCollector() const
    {
        return collector;
    }

    void close();
private:
    bool isLeftOuterJoin;
    GeneratedCsvLookupFunction<int64_t>* fetcher;
    TableFunctionCollector* collector;
    Collector* innerCollector; // The actual collector inside TableFunctionCollector.

    // inputTypeInfos is the type of incoming probe side
    nlohmann::json description;
};


#endif
