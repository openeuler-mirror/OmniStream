/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef TASKMANAGERSERVICECONFIGURATIONPOD_H
#define TASKMANAGERSERVICECONFIGURATIONPOD_H


#include <string>
#include <iostream>
#include <nlohmann/json.hpp>

#include "ResourceIDPOD.h"
namespace omnistream {
class TaskManagerServiceConfigurationPOD {

public:
    // Default constructor
    TaskManagerServiceConfigurationPOD() :
        memorySize(0), pageSize(0), requestSegmentsTimeoutMillis(0),
        numIoThreads(0), externalAddress(""), localCommunicationOnly(false),networkbuffersPerChannel(0), partitionRequestMaxBackoff(0),
    partitionRequestInitialBackoff(0),floatingNetworkbuffersPerGate(0),segmentSize(0),numberofSegmentsGlobal(0),sortShuffleMinBuffers(0),sortShuffleMinParallelism(0)
    {}

    // Copy constructor
    TaskManagerServiceConfigurationPOD(const TaskManagerServiceConfigurationPOD& other) :
        resourceID(other.resourceID),
        memorySize(other.memorySize),
        pageSize(other.pageSize),
        requestSegmentsTimeoutMillis(other.requestSegmentsTimeoutMillis),
        numIoThreads(other.numIoThreads),
        externalAddress(other.externalAddress),
        localCommunicationOnly(other.localCommunicationOnly),
        networkbuffersPerChannel(other.networkbuffersPerChannel),
        partitionRequestMaxBackoff(other.partitionRequestMaxBackoff),
        partitionRequestInitialBackoff(0),
        floatingNetworkbuffersPerGate(other.floatingNetworkbuffersPerGate),
        segmentSize(other.segmentSize),
        numberofSegmentsGlobal(other.numberofSegmentsGlobal),
        sortShuffleMinBuffers(other.sortShuffleMinBuffers),
        sortShuffleMinParallelism(other.sortShuffleMinParallelism)
    {}
    TaskManagerServiceConfigurationPOD(const ResourceIDPOD &resource_id, long memory_size, int page_size,
        long request_segments_timeout_millis, int num_io_threads, const std::string &external_address,
        bool local_communication_only, int networkbuffers_per_channel,
        int partition_request_initial_backoff, int partition_request_max_backoff,
        int floating_networkbuffers_per_gate, int segment_size,
        int numberof_segments_global, int sortShuffleMinBuffers, int sortShuffleMinParallelism)
    : resourceID(resource_id),
      memorySize(memory_size),
      pageSize(page_size),
      requestSegmentsTimeoutMillis(request_segments_timeout_millis),
      numIoThreads(num_io_threads),
      externalAddress(external_address),
      localCommunicationOnly(local_communication_only),
      networkbuffersPerChannel(networkbuffers_per_channel),
      partitionRequestMaxBackoff(partition_request_max_backoff),
      partitionRequestInitialBackoff(partition_request_initial_backoff),
      floatingNetworkbuffersPerGate(floating_networkbuffers_per_gate),
      segmentSize(segment_size),
      numberofSegmentsGlobal(numberof_segments_global),
      sortShuffleMinBuffers(sortShuffleMinBuffers), sortShuffleMinParallelism(sortShuffleMinParallelism)
    {
    }

    // Getters
    const ResourceIDPOD& getResourceID() const { return resourceID; }
    long getMemorySize() const { return memorySize; }
    int getPageSize() const { return pageSize; }
    long getRequestSegmentsTimeoutMillis() const { return requestSegmentsTimeoutMillis; }
    int getNumIoThreads() const { return numIoThreads; }
    const std::string& getExternalAddress() const { return externalAddress; }
    bool isLocalCommunicationOnly() const { return localCommunicationOnly; }

    int getNetworkBuffersPerChannel() const
    {
        return networkbuffersPerChannel;
    }

    int getPartitionRequestInitialBackoff() const
    {
        return partitionRequestMaxBackoff;
    }

    int getPartitionRequestMaxBackoff() const
    {
        return partitionRequestMaxBackoff;
    }

    int getFloatingNetworkBuffersPerGate() const
    {
        return floatingNetworkbuffersPerGate;
    }

    int getSegmentSize() const
    {
        return segmentSize;
    }

    int getNumberofSegmentsGlobal() const
    {
        return numberofSegmentsGlobal;
    }

    int getsortShuffleMinParallelism() const
    {
        return sortShuffleMinParallelism;
    }

    int getsortShuffleMinBuffers() const
    {
        return sortShuffleMinBuffers;
    }

    // Setters
    void setResourceID(const ResourceIDPOD& resourceID)
    {
        this->resourceID = resourceID;
    }
    void setMemorySize(long memorySize)
    {
        this->memorySize = memorySize;
    }
    void setPageSize(int pageSize)
    {
        this->pageSize = pageSize;
    }
    void setRequestSegmentsTimeoutMillis(long requestSegmentsTimeoutMillis)
    {
        this->requestSegmentsTimeoutMillis = requestSegmentsTimeoutMillis;
    }
    void setNumIoThreads(int numIoThreads)
    {
        this->numIoThreads = numIoThreads;
    }
    void setExternalAddress(const std::string& externalAddress)
    {
        this->externalAddress = externalAddress;
    }
    void setLocalCommunicationOnly(bool localCommunicationOnly)
    {
        this->localCommunicationOnly = localCommunicationOnly;
    }

    void setnetworkbuffersPerChannel(int networkbuffersPerChannel)
    {
        this->networkbuffersPerChannel=networkbuffersPerChannel;
    }
    void setpartitionRequestInitialBackoff(int setpartitionRequestInitialBackoff)
    {
        this->partitionRequestInitialBackoff=partitionRequestInitialBackoff;
    }
    void setpartitionRequestMaxBackoff(int partitionRequestMaxBackoff)
    {
        this->partitionRequestMaxBackoff=partitionRequestMaxBackoff;
    }
    void setfloatingNetworkbuffersPerGate(int floatingNetworkbuffersPerGate)
    {
        this->floatingNetworkbuffersPerGate=floatingNetworkbuffersPerGate;
    }
    void setsegmentSize(int segmentSize)
    {
        this->segmentSize=segmentSize;
    }
    void setnumberofSegmentsGlobal(int numberofSegmentsGlobal)
    {
        this->numberofSegmentsGlobal=numberofSegmentsGlobal;
    }

    void setsortShuffleMinParallelism(int sortShuffleMinParallelism)
    {
        this->sortShuffleMinParallelism=sortShuffleMinParallelism;
    }
    void setsortShuffleMinBuffers(int sortShuffleMinBuffers)
    {
        this->sortShuffleMinBuffers=sortShuffleMinBuffers;
    }

    // toString method
    std::string toString() const
    {
        return "Resource ID: " + resourceID.toString() + // Assuming ResourceIDPOJO has a toString()
               ", Memory Size: " + std::to_string(memorySize) +
               ", Page Size: " + std::to_string(pageSize) +
               ", Request Timeout: " + std::to_string(requestSegmentsTimeoutMillis) +
               ", IO Threads: " + std::to_string(numIoThreads) +
               ", External Address: " + externalAddress +
               ", Local Communication Only: " + (localCommunicationOnly ? "true" : "false")+
                   ", networkbuffersPerChannel: " + std::to_string(networkbuffersPerChannel)+
                       ", partitionRequestMaxBackoff: " + std::to_string(partitionRequestMaxBackoff)+
                           ", floatingNetworkbuffersPerGate: " + std::to_string(floatingNetworkbuffersPerGate)+
                               ", numberofSegmentsGlobal: " + std::to_string(numberofSegmentsGlobal)+
                                    ", segmentSize: " + std::to_string(segmentSize);


    }

    // JSON serialization/deserialization using NLOHMANN_DEFINE_TYPE_INTRUSIVE
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(TaskManagerServiceConfigurationPOD,
                                    resourceID, memorySize, pageSize,
                                    requestSegmentsTimeoutMillis, numIoThreads,
                                    externalAddress, localCommunicationOnly,
                                    networkbuffersPerChannel, partitionRequestInitialBackoff,
                                    partitionRequestMaxBackoff, floatingNetworkbuffersPerGate,
                                    numberofSegmentsGlobal, segmentSize,
                                    sortShuffleMinParallelism, sortShuffleMinBuffers)
private:
    ResourceIDPOD resourceID;
    long memorySize;
    int pageSize;
    long requestSegmentsTimeoutMillis;
    int numIoThreads;
    std::string externalAddress;
    bool localCommunicationOnly;

    int networkbuffersPerChannel;
    int partitionRequestMaxBackoff;
    int partitionRequestInitialBackoff;
    int floatingNetworkbuffersPerGate;
    int segmentSize;
    int numberofSegmentsGlobal;

    int sortShuffleMinBuffers;
    int sortShuffleMinParallelism;
};

} // namespace omnistream

#endif // TASKMANAGERSERVICECONFIGURATIONPOD_H
