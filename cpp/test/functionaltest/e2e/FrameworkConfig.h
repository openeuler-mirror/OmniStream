#include <gtest/gtest.h>

#include "runtime/partitioner/ForwardPartitionerV2.h"
#include "runtime/partitioner/RebalancePartitionerV2.h"
#include "runtime/partitioner/RescalePartitionerV2.h"
#include "runtime/partitioner/StreamPartitionerV2.h"
#include "core/writer/vectorBatchTestUtils.h"
#include "nlohmann/json.hpp"
#include "runtime/executiongraph/StreamPartitionerPOD.h"
#include "runtime/taskexecutor/OmniTaskExecutor.h"
#include "runtime/taskmanager/OmniTask.h"
#include "runtime/tasks/OmniStreamTask.h"

class FrameworkTestConfig : public ::testing::Test {
public:
    void SetUp() override {
        clearOutputFile();
    }

    void TearDown() override {
        clearOutputFile();
        ::testing::Test::TearDown();
    }

    static void clearOutputFile() {
        std::ofstream ofs("/tmp/flink_output.txt", std::ofstream::out | std::ofstream::trunc);
        ofs.close();
    }

    static std::string getOutputFile() {
        std::ifstream ifs("/tmp/flink_output.txt");
        std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
        return content;
    }

    static std::string getPartitionerName(const omnistream::TaskInformationPOD& taskInfo) {
        return taskInfo.getStreamConfigPOD().getOutEdgesInOrder()[0].getPartitioner().getPartitionerName();
    }

    /**
     * Helper funciton to create partitioner from partitioner name, copied from `runtime/tasks/OmniStreamTask.cpp`
     */
    static std::shared_ptr<omnistream::StreamPartitionerV2<StreamRecord>> getPartitionerFromPartitionerName(const std::string& partitionerName) {
        if (partitionerName == omnistream::StreamPartitionerPOD::FORWARD) {
            return std::make_shared<omnistream::ForwardPartitionerV2<StreamRecord>>();
        } else if (partitionerName == omnistream::StreamPartitionerPOD::REBALANCE) {
            return std::make_shared<omnistream::RebalancePartitionerV2<StreamRecord>>();
        } else if (partitionerName == omnistream::StreamPartitionerPOD::RESCALE) {
            return std::make_shared<omnistream::RescalePartitionerV2<StreamRecord>>();
        } else {
            throw std::invalid_argument("Invalid partitioner!");
        }
    }

    static std::shared_ptr<StreamRecord> createDummyRecord(int numRows, std::vector<omniruntime::type::DataTypeId>& types) {
        auto vectorBatch = omnistream::createVectorBatch(types, numRows, true);
        // omniruntime::vec::VectorHelper::PrintVecBatch(vectorBatch);
        return std::make_shared<StreamRecord>(vectorBatch);
    }

protected:
    std::shared_ptr<omnistream::TaskManagerServices> taskManagerServices_;
    std::shared_ptr<omnistream::OmniTaskExecutor> taskExecutor_;
    std::shared_ptr<omnistream::OmniTask> sourceTask_;
    std::shared_ptr<omnistream::OmniTask> sinkTask_;
    std::shared_ptr<omnistream::OmniStreamTask> sourceStreamTask_;
    std::shared_ptr<omnistream::OmniStreamTask> sinkStreamTask_;

    nlohmann::json taskMCSConfJson_;
    omnistream::JobInformationPOD jobInfo_;
    omnistream::TaskInformationPOD srcTaskInfo_;
    omnistream::TaskDeploymentDescriptorPOD srcTddInfo_;
    omnistream::TaskInformationPOD sinkTaskInfo_;
    omnistream::TaskDeploymentDescriptorPOD sinkTddInfo_;
};