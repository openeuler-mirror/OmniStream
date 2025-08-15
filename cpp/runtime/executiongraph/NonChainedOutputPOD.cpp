/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */


// NonChainedOutputPOD.cpp
#include "NonChainedOutputPOD.h"
#include <sstream>


namespace omnistream {

NonChainedOutputPOD::NonChainedOutputPOD()
    : sourceNodeId(0), consumerParallelism(0), consumerMaxParallelism(0),
      bufferTimeout(0), dataSetId(), isPersistentDataSet(false), partitioner(), partitionType(0) {}

NonChainedOutputPOD::NonChainedOutputPOD(int sourceNodeId, int consumerParallelism,
                                         int consumerMaxParallelism, long bufferTimeout,
                                         IntermediateDataSetIDPOD dataSetId, bool isPersistentDataSet,
                                         StreamPartitionerPOD partitioner, int partitionType)
    : sourceNodeId(sourceNodeId), consumerParallelism(consumerParallelism),
      consumerMaxParallelism(consumerMaxParallelism), bufferTimeout(bufferTimeout),
      dataSetId(dataSetId), isPersistentDataSet(isPersistentDataSet),
      partitioner(partitioner), partitionType(partitionType) {}

NonChainedOutputPOD::NonChainedOutputPOD(const NonChainedOutputPOD& other)
    : sourceNodeId(other.sourceNodeId), consumerParallelism(other.consumerParallelism),
      consumerMaxParallelism(other.consumerMaxParallelism), bufferTimeout(other.bufferTimeout),
      dataSetId(other.dataSetId), isPersistentDataSet(other.isPersistentDataSet),
      partitioner(other.partitioner), partitionType(other.partitionType) {}

NonChainedOutputPOD& NonChainedOutputPOD::operator=(const NonChainedOutputPOD& other)
{
    if (this != &other) {
        sourceNodeId = other.sourceNodeId;
        consumerParallelism = other.consumerParallelism;
        consumerMaxParallelism = other.consumerMaxParallelism;
        bufferTimeout = other.bufferTimeout;
        dataSetId = other.dataSetId;
        isPersistentDataSet = other.isPersistentDataSet;
        partitioner = other.partitioner;
        partitionType = other.partitionType;
    }
    return *this;
}

NonChainedOutputPOD::~NonChainedOutputPOD() {}

int NonChainedOutputPOD::getSourceNodeId() const
{
    return sourceNodeId;
}

void NonChainedOutputPOD::setSourceNodeId(int sourceNodeId)
{
    this->sourceNodeId = sourceNodeId;
}

int NonChainedOutputPOD::getConsumerParallelism() const
{
    return consumerParallelism;
}

void NonChainedOutputPOD::setConsumerParallelism(int consumerParallelism)
{
    this->consumerParallelism = consumerParallelism;
}

int NonChainedOutputPOD::getConsumerMaxParallelism() const
{
    return consumerMaxParallelism;
}

void NonChainedOutputPOD::setConsumerMaxParallelism(int consumerMaxParallelism)
{
    this->consumerMaxParallelism = consumerMaxParallelism;
}

long NonChainedOutputPOD::getBufferTimeout() const
{
    return bufferTimeout;
}

void NonChainedOutputPOD::setBufferTimeout(long bufferTimeout)
{
    this->bufferTimeout = bufferTimeout;
}

IntermediateDataSetIDPOD NonChainedOutputPOD::getDataSetId() const
{
    return dataSetId;
}

void NonChainedOutputPOD::setDataSetId(const IntermediateDataSetIDPOD& dataSetId)
{
    this->dataSetId = dataSetId;
}

bool NonChainedOutputPOD::getIsPersistentDataSet() const
{
    return isPersistentDataSet;
}

void NonChainedOutputPOD::setIsPersistentDataSet(bool isPersistentDataSet)
{
    this->isPersistentDataSet = isPersistentDataSet;
}

StreamPartitionerPOD NonChainedOutputPOD::getPartitioner() const
{
    return partitioner;
}

void NonChainedOutputPOD::setPartitioner(const StreamPartitionerPOD& partitioner)
{
    this->partitioner = partitioner;
}

int NonChainedOutputPOD::getPartitionType() const
{
    return partitionType;
}

void NonChainedOutputPOD::setPartitionType(int partitionType)
{
    this->partitionType = partitionType;
}

std::string NonChainedOutputPOD::toString() const
{
    std::stringstream ss;
    ss << "NonChainedOutputPOD{"
       << "sourceNodeId=" << sourceNodeId << ", consumerParallelism=" << consumerParallelism
       << ", consumerMaxParallelism=" << consumerMaxParallelism << ", bufferTimeout=" << bufferTimeout
       << ", dataSetId=" << dataSetId.toString() << ", isPersistentDataSet=" << isPersistentDataSet
       << ", partitioner=" << partitioner.toString() << ", partitionType=" << partitionType << "}";
    return ss.str();
}

} // namespace omnistream
