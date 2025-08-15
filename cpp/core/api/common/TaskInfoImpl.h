/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_TASKINFOIMPL_H
#define FLINK_TNEL_TASKINFOIMPL_H

#include <string>
#include <cstring>
#include <vector>
#include "TaskInfo.h"

class TaskInfoImpl : public TaskInfo
{
public:
    TaskInfoImpl(std::string taskName,
                 int maxNumberOfParallelSubtasks,
                 int numberOfParallelSubtasks,
                 int indexOfSubtask) : taskName(taskName),
                                       maxNumberOfParallelSubtasks(maxNumberOfParallelSubtasks),
                                       indexOfSubtask(indexOfSubtask),
                                       numberOfParallelSubtasks(numberOfParallelSubtasks) {
        const char* envVar = std::getenv("RocksDBBackend");
        if (envVar != nullptr) {
            if (strlen(envVar) != 0 && strcmp(envVar, "true") == 0) {
                stateBackend = "RocksDBStateBackend";
            }
        }
    };

    TaskInfoImpl(std::string taskName,
                 int maxNumberOfParallelSubtasks,
                 int numberOfParallelSubtasks,
                 int indexOfSubtask, std::string stateBackend) : taskName(taskName),
                                                                 maxNumberOfParallelSubtasks(
                                                                         maxNumberOfParallelSubtasks),
                                                                 indexOfSubtask(
                                                                         indexOfSubtask),
                                                                 numberOfParallelSubtasks(
                                                                         numberOfParallelSubtasks),
                                                                 stateBackend(
                                                                         stateBackend) {};

    TaskInfoImpl(std::string taskName,
                 int maxNumberOfParallelSubtasks,
                 int numberOfParallelSubtasks,
                 int indexOfSubtask, std::string stateBackend,
                 std::vector<std::string> backendHomes) : taskName(taskName),
                                                          maxNumberOfParallelSubtasks(
                                                                  maxNumberOfParallelSubtasks),
                                                          indexOfSubtask(
                                                                  indexOfSubtask),
                                                          numberOfParallelSubtasks(
                                                                  numberOfParallelSubtasks),
                                                          stateBackend(
                                                                  stateBackend) {
        if (backendHomes.size() == 0) {
            backendHome = "/tmp/rocksdb";
        } else {
            backendHome = backendHomes[0];
        }
    };

    std::string getTaskName() override { return taskName; };
    int getMaxNumberOfParallelSubtasks() override { return maxNumberOfParallelSubtasks; };
    int getIndexOfThisSubtask() override { return indexOfSubtask; };
    int getNumberOfParallelSubtasks() override { return numberOfParallelSubtasks; };
    std::string getStateBackend() override { return stateBackend; }
    std::string getBackendHome() override { return backendHome; }
private:
    std::string taskName;
    int maxNumberOfParallelSubtasks;
    int indexOfSubtask;
    int numberOfParallelSubtasks;
    std::string stateBackend = "HashMapStateBackend"; // HashMapStateBackend
    std::string backendHome = "/tmp/rocksdb";
};

#endif // FLINK_TNEL_TASKINFOIMPL_H
