/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef TASKINFORMATIONPOD_H
#define TASKINFORMATIONPOD_H

#include <string>
#include <nlohmann/json.hpp>
#include "StreamConfigPOD.h"
#include "operatorchain/OperatorChainPOD.h"

namespace omnistream {

class TaskInformationPOD {
public:
    TaskInformationPOD() = default;

    TaskInformationPOD(const std::string &taskName, int numberOfSubtasks, int maxNumberOfSubtasks, int indexOfSubtask,
                       const StreamConfigPOD &streamConfigPOD, const OperatorChainPOD &operatorChain,
                       const std::vector<StreamConfigPOD> &chainedConfig_ )
        : taskName(taskName), numberOfSubtasks(numberOfSubtasks), maxNumberOfSubtasks(maxNumberOfSubtasks),
        indexOfSubtask(indexOfSubtask), streamConfig(streamConfigPOD), chainedConfig(chainedConfig_)
    {}

    TaskInformationPOD(const std::string &taskName, int numberOfSubtasks, int maxNumberOfSubtasks, int indexOfSubtask,
                       const StreamConfigPOD &streamConfigPOD, const OperatorChainPOD &operatorChain,
                       const std::vector<StreamConfigPOD> &chainedConfig_, std::string &stateBackend,
                       std::vector<std::string> rocksdbStorePaths)
        : taskName(taskName), numberOfSubtasks(numberOfSubtasks), maxNumberOfSubtasks(maxNumberOfSubtasks),
        indexOfSubtask(indexOfSubtask), stateBackend(stateBackend), rocksdbStorePaths(rocksdbStorePaths),
        streamConfig(streamConfigPOD), chainedConfig(chainedConfig_)
    {}

    TaskInformationPOD(const TaskInformationPOD &other) = default;

    std::string getTaskName() const
    {
        return taskName;
    }

    void setTaskName(const std::string &taskName)
    {
        this->taskName = taskName;
    }

    int getNumberOfSubtasks() const
    {
        return numberOfSubtasks;
    }

    void setNumberOfSubtasks(int numberOfSubtasks)
    {
        this->numberOfSubtasks = numberOfSubtasks;
    }

    int getIndexOfSubtask() const
    {
        return indexOfSubtask;
    }

    void setIndexOfSubtask(int indexOfSubtask)
    {
        this->indexOfSubtask = indexOfSubtask;
    }

    int getMaxNumberOfSubtasks() const
    {
        return maxNumberOfSubtasks;
    }

    void setMaxNumberOfSubtasks(int maxNumberOfSubtasks)
    {
        this->maxNumberOfSubtasks = maxNumberOfSubtasks;
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

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(
        TaskInformationPOD, taskName, numberOfSubtasks, maxNumberOfSubtasks, indexOfSubtask, stateBackend,
        rocksdbStorePaths,  streamConfig, chainedConfig)
private:
    std::string taskName;
    int numberOfSubtasks;
    int maxNumberOfSubtasks;
    int indexOfSubtask;
    std::string stateBackend;
    std::vector<std::string> rocksdbStorePaths;
    StreamConfigPOD streamConfig;
    std::unordered_map<int, StreamConfigPOD> chainedConfigMap;
    std::vector<StreamConfigPOD> chainedConfig;
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
