/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef TASKPLAININFOPOD_H
#define TASKPLAININFOPOD_H


#include <string>
#include <nlohmann/json.hpp>

namespace omnistream {

class TaskPlainInfoPOD {
public:
  // Default constructor
  TaskPlainInfoPOD() :
    taskName(""),
    taskNameWithSubtasks(""),
    allocationIDAsString(""),
    maxNumberOfParallelSubtasks(0),
    indexOfSubtask(0),
    numberOfParallelSubtasks(0),
    attemptNumber(0)
  {}

  // Full argument constructor
  TaskPlainInfoPOD(const std::string& taskName, const std::string& taskNameWithSubtasks,
                   const std::string& allocationIDAsString, int maxNumberOfParallelSubtasks,
                   int indexOfSubtask, int numberOfParallelSubtasks, int attemptNumber) :
    taskName(taskName),
    taskNameWithSubtasks(taskNameWithSubtasks),
    allocationIDAsString(allocationIDAsString),
    maxNumberOfParallelSubtasks(maxNumberOfParallelSubtasks),
    indexOfSubtask(indexOfSubtask),
    numberOfParallelSubtasks(numberOfParallelSubtasks),
    attemptNumber(attemptNumber)
  {}

  // Copy constructor
  TaskPlainInfoPOD(const TaskPlainInfoPOD& other) = default;

  // Getters
  const std::string& getTaskName() const { return taskName; }
  const std::string& getTaskNameWithSubtasks() const { return taskNameWithSubtasks; }
  const std::string& getAllocationIDAsString() const { return allocationIDAsString; }
  int getMaxNumberOfParallelSubtasks() const { return maxNumberOfParallelSubtasks; }
  int getIndexOfSubtask() const { return indexOfSubtask; }
  int getNumberOfParallelSubtasks() const { return numberOfParallelSubtasks; }
  int getAttemptNumber() const { return attemptNumber; }

  // Setters
  void setTaskName(const std::string& taskName) { this->taskName = taskName; }
  void setTaskNameWithSubtasks(const std::string& taskNameWithSubtasks) { this->taskNameWithSubtasks = taskNameWithSubtasks; }
  void setAllocationIDAsString(const std::string& allocationIDAsString) { this->allocationIDAsString = allocationIDAsString; }
  void setMaxNumberOfParallelSubtasks(int maxNumberOfParallelSubtasks) { this->maxNumberOfParallelSubtasks = maxNumberOfParallelSubtasks; }
  void setIndexOfSubtask(int indexOfSubtask) { this->indexOfSubtask = indexOfSubtask; }
  void setNumberOfParallelSubtasks(int numberOfParallelSubtasks) { this->numberOfParallelSubtasks = numberOfParallelSubtasks; }
  void setAttemptNumber(int attemptNumber) { this->attemptNumber = attemptNumber; }

  // toString method
  std::string toString() const
  {
    return "TaskPlainInfoPOD{ taskName='" + taskName + '\'' +
           ", taskNameWithSubtasks='" + taskNameWithSubtasks + '\'' +
           ", allocationIDAsString='" + allocationIDAsString + '\'' +
           ", maxNumberOfParallelSubtasks=" + std::to_string(maxNumberOfParallelSubtasks) +
           ", indexOfSubtask=" + std::to_string(indexOfSubtask) +
           ", numberOfParallelSubtasks=" + std::to_string(numberOfParallelSubtasks) +
           ", attemptNumber=" + std::to_string(attemptNumber) +
           '}';
  }

  bool operator==(const TaskPlainInfoPOD& other) const
  {
    return taskName == other.taskName && taskNameWithSubtasks == other.taskNameWithSubtasks && allocationIDAsString == other.allocationIDAsString &&
      maxNumberOfParallelSubtasks == other.maxNumberOfParallelSubtasks && indexOfSubtask == other.indexOfSubtask && numberOfParallelSubtasks == other.numberOfParallelSubtasks && attemptNumber == other.attemptNumber;
  }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(TaskPlainInfoPOD, taskName, taskNameWithSubtasks, allocationIDAsString,
                                maxNumberOfParallelSubtasks, indexOfSubtask, numberOfParallelSubtasks,
                                attemptNumber)
private:
  std::string taskName;
  std::string taskNameWithSubtasks;
  std::string allocationIDAsString;
  int maxNumberOfParallelSubtasks;
  int indexOfSubtask;
  int numberOfParallelSubtasks;
  int attemptNumber;
};

} // namespace omnistream

namespace std {
  template <>
  struct hash<omnistream::TaskPlainInfoPOD> {
    std::size_t operator()(const omnistream::TaskPlainInfoPOD& obj) const
    {
      // std::string taskName;
      // std::string taskNameWithSubtasks;
      // std::string allocationIDAsString;
      // int maxNumberOfParallelSubtasks;
      // int indexOfSubtask;
      // int numberOfParallelSubtasks;
      // int attemptNumber;
      size_t seed=0;
      size_t h1=std::hash<std::string>()(obj.getTaskName());
      size_t h2=std::hash<std::string>()(obj.getTaskNameWithSubtasks());
      size_t h3=std::hash<std::string>()(obj.getAllocationIDAsString());
      size_t h4=std::hash<int>()(obj.getMaxNumberOfParallelSubtasks());
      size_t h5=std::hash<int>()(obj.getIndexOfSubtask());
      size_t h6=std::hash<int>()(obj.getNumberOfParallelSubtasks());
      size_t h7=std::hash<int>()(obj.getAttemptNumber());
      seed ^= h1 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= h3 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= h4 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= h5 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= h6 + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= h7 + 0x9e3779b9 + (seed << 6) + (seed >> 2);

      return seed;
    }
  };
}


#endif //TASKPLAININFOPOD_H
