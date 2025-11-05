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

#ifndef TASKINFORMATIONPOD_H
#define TASKINFORMATIONPOD_H

#include <string>
#include <filesystem>
#include <nlohmann/json.hpp>
#include "StreamConfigPOD.h"
#include "CheckpointConfigPOD.h"
#include "ExecutionCheckpointConfigPOD.h"
#include "operatorchain/OperatorChainPOD.h"

namespace omnistream {

class TaskInformationPOD {
public:
    TaskInformationPOD()
        : taskName(""), numberOfSubtasks(1), maxNumberOfSubtasks(1), indexOfSubtask(0),
          streamConfig(StreamConfigPOD()), chainedConfig(), stateBackend(""),
          rocksdbStorePaths(), taskType(1), checkpointConfig(CheckpointConfigPOD()),
          executionCheckpointConfig(ExecutionCheckpointConfigPOD()), localRecoveryConfig(""),
          tmpWorkingDirectory("/tmp") {}

    TaskInformationPOD(const std::string &taskName, int numberOfSubtasks, int maxNumberOfSubtasks, int indexOfSubtask,
                       const StreamConfigPOD &streamConfigPOD, const OperatorChainPOD &operatorChain,
                       const std::vector<StreamConfigPOD> &chainedConfig_, const int taskType = 1)
        : taskName(taskName), numberOfSubtasks(numberOfSubtasks), maxNumberOfSubtasks(maxNumberOfSubtasks),
          indexOfSubtask(indexOfSubtask), streamConfig(streamConfigPOD), chainedConfig(chainedConfig_),
          taskType(taskType) {
    }

    TaskInformationPOD(const std::string &taskName, int numberOfSubtasks, int maxNumberOfSubtasks, int indexOfSubtask,
                       const StreamConfigPOD &streamConfigPOD, const OperatorChainPOD &operatorChain,
                       const std::vector<StreamConfigPOD> &chainedConfig_, std::string &stateBackend,
                       std::vector<std::string> rocksdbStorePaths)
        : taskName(taskName), numberOfSubtasks(numberOfSubtasks), maxNumberOfSubtasks(maxNumberOfSubtasks),
        indexOfSubtask(indexOfSubtask), streamConfig(streamConfigPOD), chainedConfig(chainedConfig_),
        stateBackend(stateBackend), rocksdbStorePaths(rocksdbStorePaths)
    {}

    TaskInformationPOD &operator=(const TaskInformationPOD&) = default;
    TaskInformationPOD(const TaskInformationPOD &other) = default;

    std::string getTaskName() const
    {
        return taskName;
    }

    void setTaskName(const std::string &taskName_)
    {
        this->taskName = taskName_;
    }

    int getNumberOfSubtasks() const
    {
        return numberOfSubtasks;
    }

    void setNumberOfSubtasks(int numberOfSubtasks_)
    {
        this->numberOfSubtasks = numberOfSubtasks_;
    }

    int getIndexOfSubtask() const
    {
        return indexOfSubtask;
    }

    void setIndexOfSubtask(int indexOfSubtask_)
    {
        this->indexOfSubtask = indexOfSubtask_;
    }

    int getMaxNumberOfSubtasks() const
    {
        return maxNumberOfSubtasks;
    }

    void setMaxNumberOfSubtasks(int maxNumberOfSubtasks_)
    {
        this->maxNumberOfSubtasks = maxNumberOfSubtasks_;
    }

    const StreamConfigPOD &getStreamConfigPOD() const
    {
        return streamConfig;
    }

    void setStreamConfigPOD(const StreamConfigPOD &streamConfigPOD)
    {
        this->streamConfig = streamConfigPOD;
    }

    [[nodiscard]] std::vector<StreamConfigPOD> getChainedConfig() const
    {
        return chainedConfig;
    }

    [[nodiscard]] std::unordered_map<int, StreamConfigPOD> getChainedConfigMap()
    {
        if (chainedConfigMap.empty()) {
            for (auto streamConfig : chainedConfig) {
                chainedConfigMap[streamConfig.getOperatorDescription().getVertexID()] = streamConfig;
            }
        }
        return chainedConfigMap;
    }

    void setChainedConfig(const std::vector<StreamConfigPOD>& chained_config)
    {
        chainedConfig = chained_config;
    }

    const std::string &getStateBackend() const
    {
        return stateBackend;
    }

    void setStateBackend(const std::string &stateBackend_)
    {
        stateBackend = stateBackend_;
    }

    const std::vector<std::string> &getRocksdbStorePaths() const
    {
        return rocksdbStorePaths;
    }

    void setRocksdbStorePaths(const std::vector<std::string> &rocksdbStorePaths_)
    {
        rocksdbStorePaths = rocksdbStorePaths_;
    }

    std::string toString() const
    {
        std::stringstream ss;
        ss << "{";

        bool first = true;
        for (const auto& streamConfig : chainedConfig) {
            if (!first) {
                ss << ", ";
            }
            ss << streamConfig.getOperatorDescription().getVertexID() << ": " << streamConfig.toString();
            first = false;
        }

        ss << "}";

        return "TaskInformationPOD{ taskName='" + taskName + '\'' +
               ", numberOfSubtasks=" + std::to_string(numberOfSubtasks) +
               ", maxNumberOfSubtasks=" + std::to_string(maxNumberOfSubtasks) +
               ", indexOfSubtask=" + std::to_string(indexOfSubtask) +
               ", streamConfigPOD=" + streamConfig.toString() +
               ", stateBackend=" + stateBackend +
                   ", chainedConfig= "  + ss.str() + '}';
    }

    bool operator==(const TaskInformationPOD& other) const
    {
        return taskName == other.taskName &&
               numberOfSubtasks == other.numberOfSubtasks &&
               maxNumberOfSubtasks == other.maxNumberOfSubtasks &&
               indexOfSubtask == other.indexOfSubtask &&
               streamConfig == other.streamConfig &&
               chainedConfig == other.chainedConfig &&
               stateBackend == other.stateBackend &&
                rocksdbStorePaths == other.rocksdbStorePaths;
    }

    int GetTaskType() const
    {
        return taskType;
    }

    void SetTaskType(const int &taskType)
    {
        this->taskType = taskType;
    }

    const CheckpointConfigPOD& getCheckpointConfig() const
    {
        return checkpointConfig;
    }

    void setCheckpointConfig(const CheckpointConfigPOD& cfg)
    {
        checkpointConfig = cfg;
    }

    const ExecutionCheckpointConfigPOD& getExecutionCheckpointConfig() const
    {
        return executionCheckpointConfig;
    }

    void setExecutionCheckpointConfig(const ExecutionCheckpointConfigPOD& cfg)
    {
        executionCheckpointConfig = cfg;
    }

    const std::string& getLocalRecoveryConfig() const
    {
        return localRecoveryConfig;
    }

    std::filesystem::path getTmpWorkingDirectory()
    {
        return tmpWorkingDirectory;
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(
        TaskInformationPOD, taskName, numberOfSubtasks, maxNumberOfSubtasks, indexOfSubtask, stateBackend,
        rocksdbStorePaths,  streamConfig, chainedConfig, taskType, checkpointConfig, executionCheckpointConfig,
        localRecoveryConfig, tmpWorkingDirectory)
private:
    std::string taskName;
    int numberOfSubtasks;
    int maxNumberOfSubtasks;
    int indexOfSubtask;
    StreamConfigPOD streamConfig;
    std::vector<StreamConfigPOD> chainedConfig;
    std::string stateBackend;
    std::vector<std::string> rocksdbStorePaths;
    std::unordered_map<int, StreamConfigPOD> chainedConfigMap;
    int taskType;
    CheckpointConfigPOD checkpointConfig;
    ExecutionCheckpointConfigPOD executionCheckpointConfig;
    std::string localRecoveryConfig;
    std::string tmpWorkingDirectory;
};

}  // namespace omnistream

namespace std {
    template<>
    struct hash<omnistream::TaskInformationPOD> {
        size_t operator()(const omnistream::TaskInformationPOD& obj) const
        {
            size_t h1 = std::hash<std::string>()(obj.getTaskName());
            size_t h2 = std::hash<int>()(obj.getNumberOfSubtasks());
            size_t h3 = std::hash<int>()(obj.getMaxNumberOfSubtasks());
            size_t h4 = std::hash<int>()(obj.getIndexOfSubtask());
            size_t h5 = std::hash<omnistream::StreamConfigPOD>()(obj.getStreamConfigPOD());

            // 对 vector<StreamConfigPOD> 做 hash，逐个元素合并
            size_t h6 = 0;
            for (const auto& cfg : obj.getChainedConfig()) {
                size_t h = std::hash<omnistream::StreamConfigPOD>()(cfg);
                h6 ^= h + 0x9e3779b9 + (h6 << 6) + (h6 >> 2);
            }

            size_t seed = 0;
            seed ^= h1 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= h3 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= h4 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= h5 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= h6 + 0x9e3779b9 + (seed << 6) + (seed >> 2);

            return seed;
        }
    };
}

#endif  // TASKINFORMATIONPOD_H
