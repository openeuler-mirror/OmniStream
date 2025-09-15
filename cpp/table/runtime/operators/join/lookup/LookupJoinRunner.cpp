/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "LookupJoinRunner.h"

void LookupJoinRunner::processBatch(omnistream::VectorBatch *in, ProcessFunction::Context *cxt, Collector *out)
{
    // This collector is the TableFunctionCollector in LookupJoinRunner
    // out is the downstream operator
    collector->setCollector(out);
    collector->setInput(in);
    collector->reset();
    fetcher->flatMap(in, collector);

    if (isLeftOuterJoin && !collector->isCollected()) {
        // todo: currently I only implemented InnerJoin
        NOT_IMPL_EXCEPTION
    }
}

void LookupJoinRunner::open(const Configuration &parameters)
{
    using namespace omniruntime::type;
    std::string temporalTableSourceSpec = description["temporalTableSourceSpec"];
    std::string connectorType = description["connectorType"];
    std::string filepath;
    // todo: check all possibility of the parameter! And how to find the file path with it
    if (connectorType == "filesystem") {
        filepath = description["connectorPath"];
    } else {
        NOT_IMPL_EXCEPTION
    }
    auto *src = new CsvTableSource(filepath, description["lookupInputTypes"].get<std::vector<std::string>>());
    // setup collector
    collector = new TableFunctionCollector();
    collector->setCollector(innerCollector);
    // setup fetcher
    auto lookupFunction = new CsvLookupFunction<int64_t>(description, src);
    fetcher = new GeneratedCsvLookupFunction<int64_t>(lookupFunction);
    fetcher->open();
}

LookupJoinRunner::LookupJoinRunner(nlohmann::json description, Collector* innerCollector)
    : innerCollector(innerCollector), description(description)
{
    isLeftOuterJoin = (description["joinType"].get<std::string>() == "LeftOuterJoin");
}

void LookupJoinRunner::close()
{}
