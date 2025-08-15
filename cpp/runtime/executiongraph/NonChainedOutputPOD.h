/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// NonChainedOutputPOD.h
#ifndef NONCHAINEDOUTPUTPOD_H
#define NONCHAINEDOUTPUTPOD_H

#include <string>
#include "StreamPartitionerPOD.h"
#include "common/IntermediateDataSetIDPOD.h"
#include "nlohmann/json.hpp"

namespace omnistream {

class NonChainedOutputPOD {
public:
    int sourceNodeId;
    int consumerParallelism;
    int consumerMaxParallelism;
    long bufferTimeout;
    IntermediateDataSetIDPOD dataSetId;
    bool isPersistentDataSet;
    StreamPartitionerPOD partitioner;
    int partitionType;

    NonChainedOutputPOD();
    NonChainedOutputPOD(int sourceNodeId, int consumerParallelism, int consumerMaxParallelism,
                        long bufferTimeout, IntermediateDataSetIDPOD dataSetId,
                        bool isPersistentDataSet, StreamPartitionerPOD partitioner, int partitionType);
    NonChainedOutputPOD(const NonChainedOutputPOD& other);
    NonChainedOutputPOD& operator=(const NonChainedOutputPOD& other);
    ~NonChainedOutputPOD();

    int getSourceNodeId() const;
    void setSourceNodeId(int sourceNodeId);

    int getConsumerParallelism() const;
    void setConsumerParallelism(int consumerParallelism);

    int getConsumerMaxParallelism() const;
    void setConsumerMaxParallelism(int consumerMaxParallelism);

    long getBufferTimeout() const;
    void setBufferTimeout(long bufferTimeout);

    IntermediateDataSetIDPOD getDataSetId() const;
    void setDataSetId(const IntermediateDataSetIDPOD& dataSetId);

    bool getIsPersistentDataSet() const;
    void setIsPersistentDataSet(bool isPersistentDataSet);

    StreamPartitionerPOD getPartitioner() const;
    void setPartitioner(const StreamPartitionerPOD& partitioner);

    int getPartitionType() const;
    void setPartitionType(int partitionType);

    std::string toString() const;

    bool operator==(const NonChainedOutputPOD&other)const
    {
        return
        this->bufferTimeout==other.bufferTimeout &&
            this->dataSetId==other.dataSetId &&
                this->partitioner==other.partitioner&&
                    this->partitionType==other.partitionType &&
                        this->isPersistentDataSet==other.isPersistentDataSet &&
                            this->sourceNodeId==other.sourceNodeId &&
                                this->consumerMaxParallelism==other.consumerMaxParallelism &&
                                    this->consumerParallelism==other.consumerParallelism;
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(NonChainedOutputPOD, sourceNodeId, consumerParallelism,
                                  consumerMaxParallelism, bufferTimeout, dataSetId,
                                  isPersistentDataSet, partitioner, partitionType)
};

} // namespace omnistream

namespace std {
    template <>
    struct hash<omnistream::NonChainedOutputPOD> {
        std::size_t operator()(const omnistream::NonChainedOutputPOD& obj) const
        {
            size_t h1=std::hash<int>{}(obj.sourceNodeId);
            size_t h2=std::hash<long>{}(obj.bufferTimeout);
            size_t h3=std::hash<int>{}(obj.consumerParallelism);
            size_t h4=std::hash<int>{}(obj.partitionType);
            size_t h5=std::hash<int>{}(obj.consumerMaxParallelism);
            size_t h8=std::hash<bool>{}(obj.isPersistentDataSet);

            size_t h6=std::hash<omnistream::IntermediateDataSetIDPOD>{}(obj.dataSetId);
            size_t h7=std::hash<omnistream::StreamPartitionerPOD>{}(obj.partitioner);
            size_t seed = 0;

            seed ^= (h1 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            seed ^= (h2 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            seed ^= (h3 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            seed ^= (h4 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            seed ^= (h5 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            seed ^= (h6 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            seed ^= (h7 + 0x9e3779b9 + (seed << 6) + (seed >> 2));
            seed ^= (h8 + 0x9e3779b9 + (seed << 6) + (seed >> 2));

            return seed;
        }
    };
} // namespace std


#endif // NONCHAINEDOUTPUTPOD_H