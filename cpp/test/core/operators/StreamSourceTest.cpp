#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "datagen/nexmark/NexmarkSourceFunction.h"
#include "streaming/api/operators/StreamSource.h"
#include "OutputTest.h"
#include "table/runtime/operators/TableOperatorConstants.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "runtime/executiongraph/operatorchain/OperatorPOD.h"
#include "taskmanager/OmniRuntimeEnvironment.h"

TEST(StreamSourceTest, NexmarkSourceFunction) {
    std::string description = R"({"format":"nexmark", "batchSize":10, "configMap":{"maxEvents":100}})";
    nlohmann::json opDescription = nlohmann::json::parse(description);
    OperatorPOD nexmarkPOD("nexmark_source", std::string(OPERATOR_NAME_STREAM_SOURCE), description, {}, {});
    nexmarkPOD.setVOperatorType(omnistream::Type_o::SQL);
    BatchOutputTest *output = new BatchOutputTest();
    //StreamTask is set to nullptr
    auto sourceOp = reinterpret_cast<StreamSource<omnistream::VectorBatch>*> (StreamOperatorFactory::createOperatorAndCollector(nexmarkPOD, output, nullptr));
    sourceOp->setup();
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    env2->setTaskConfiguration(*taskInfo);
    sourceOp->initializeState(new StreamTaskStateInitializerImpl(env2), new IntSerializer());
    sourceOp->open();
    sourceOp->run();
    auto generatedOp = output->getVectorBatch();

}