//
// Created by xichen on 3/5/25.
//
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "datagen/nexmark/NexmarkSourceFunction.h"
#include "core/operators/StreamSource.h"
#include "OutputTest.h"
#include "core/graph/OperatorChainConfig.h"
#include "table/runtime/operators/TableOperatorConstants.h"
#include "core/operators/StreamOperatorFactory.h"
#include "runtime/executiongraph/operatorchain/OperatorPOD.h"
#include "taskmanager/RuntimeEnvironment.h"

TEST(StreamSourceTest, NexmarkSourceFunction) {
    std::string description = R"({"format":"nexmark", "batchSize":10, "configMap":{"maxEvents":100}})";
    nlohmann::json opDescription = nlohmann::json::parse(description);
    OperatorPOD nexmarkPOD("nexmark_source", std::string(OPERATOR_NAME_STREAM_SOURCE), description, {}, {});
    BatchOutputTest *output = new BatchOutputTest();
    //StreamTask is set to nullptr
    auto sourceOp = reinterpret_cast<StreamSource<omnistream::VectorBatch>*> (StreamOperatorFactory::createOperatorAndCollector(nexmarkPOD, output, nullptr));
    sourceOp->setup();
    sourceOp->initializeState(new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("test", 2, 1, 0))), new IntSerializer());
    sourceOp->open();
    sourceOp->run();
    auto generatedOp = output->getVectorBatch();

}