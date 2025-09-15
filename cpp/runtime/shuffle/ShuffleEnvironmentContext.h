/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef SHUFFLEENVIRONMENTCONTEXT_H
#define SHUFFLEENVIRONMENTCONTEXT_H
#include <executiongraph/descriptor/ResourceIDPOD.h>
#include <executiongraph/descriptor/TaskManagerServiceConfigurationPOD.h>


namespace omnistream {
    class ShuffleEnvironmentContext {

    public:
    // Default constructor
    ShuffleEnvironmentContext() : memorySize(0), pageSize(0), requestSegmentsTimeoutMillis(0) {}

    // Full argument constructor
        //todo: add some nettyshuffleenvironmentconfiguration parameters to the constructor
    ShuffleEnvironmentContext(const ResourceIDPOD& resourceID, long memorySize, int pageSize,
                              long requestSegmentsTimeoutMillis,
                              int networkbuffersPerChannel, int partitionRequestInitialBackoff,
                              int partitionRequestMaxBackoff,int floatingNetworkbuffersPerGate,
                              int segmentSize,int numberofSegmentsGlobal,int sortShuffleMinBuffers,int sortShuffleMinParallelism
                              ) :
        resourceID(resourceID), memorySize(memorySize), pageSize(pageSize),
        requestSegmentsTimeoutMillis(requestSegmentsTimeoutMillis),
        networkbuffersPerChannel(networkbuffersPerChannel),
        partitionRequestInitialBackoff(partitionRequestInitialBackoff),
        partitionRequestMaxBackoff(partitionRequestMaxBackoff),floatingNetworkbuffersPerGate(floatingNetworkbuffersPerGate),
        segmentSize(segmentSize),numberofSegmentsGlobal(numberofSegmentsGlobal),
        sortShuffleMinBuffers(sortShuffleMinBuffers),sortShuffleMinParallelism(sortShuffleMinParallelism)
        {}

    // Copy constructor
    ShuffleEnvironmentContext(const ShuffleEnvironmentContext& other) :
        resourceID(other.resourceID),
        memorySize(other.memorySize),
        pageSize(other.pageSize),
        requestSegmentsTimeoutMillis(other.requestSegmentsTimeoutMillis),
        sortShuffleMinBuffers(other.sortShuffleMinBuffers),
        sortShuffleMinParallelism(other.sortShuffleMinParallelism) {}

    // Getters
    const ResourceIDPOD& getResourceID() const { return resourceID; }
    long getMemorySize() const { return memorySize; }
    int getPageSize() const { return pageSize; }
    long getRequestSegmentsTimeoutMillis() const { return requestSegmentsTimeoutMillis; }

        int getNetworkBuffersPerChannel() const {return networkbuffersPerChannel;}
        int getPartitionRequestInitialBackoff() const {return partitionRequestInitialBackoff;}
        int getPartitionRequestMaxBackoff() const {return partitionRequestMaxBackoff;}
        int getFloatingNetworkBuffersPerGate() const {return floatingNetworkbuffersPerGate;}
        int getSegmentSize() const {return segmentSize;}
        int getNumberofSegmentsGlobal() const {return numberofSegmentsGlobal;}
    int getsortShuffleMinBuffers() const {return sortShuffleMinBuffers;}
        int getsortShuffleMinParallelism() const {return sortShuffleMinParallelism;}

    // Setters

    void setResourceID(const ResourceIDPOD& resourceID) { this->resourceID = resourceID; }
    void setMemorySize(long memorySize) { this->memorySize = memorySize; }
    void setPageSize(int pageSize) { this->pageSize = pageSize; }
    void setRequestSegmentsTimeoutMillis(long requestSegmentsTimeoutMillis) { this->requestSegmentsTimeoutMillis = requestSegmentsTimeoutMillis; }
        void setsortShuffleMinParallelism(int sortShuffleMinParallelism)
        {
            this->sortShuffleMinParallelism = sortShuffleMinParallelism;
        }
        void setsortShuffleMinBuffers(int sortShuffleMinBuffers)
        {
            this->sortShuffleMinBuffers = sortShuffleMinBuffers;
        }

    // toString method
    std::string toString() const {
        return "Resource ID: " + resourceID.toString() + // Assuming ResourceIDPOD has a toString()
               ", Memory Size: " + std::to_string(memorySize) +
               ", Page Size: " + std::to_string(pageSize) +
               ", Request Timeout: " + std::to_string(requestSegmentsTimeoutMillis);
    }

    // JSON serialization/deserialization using NLOHMANN_DEFINE_TYPE_INTRUSIVE
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(ShuffleEnvironmentContext, resourceID, memorySize, pageSize,
                                    requestSegmentsTimeoutMillis, networkbuffersPerChannel,
                                    partitionRequestInitialBackoff,
                                    partitionRequestMaxBackoff,
                                    floatingNetworkbuffersPerGate,
                                    segmentSize, numberofSegmentsGlobal,
                                    sortShuffleMinBuffers, sortShuffleMinParallelism)
    private:
    ResourceIDPOD resourceID;
    long memorySize;
    int pageSize;
    long requestSegmentsTimeoutMillis;

     //todo:add some nettyshuffleenvironmentconfiguration parameters
     int networkbuffersPerChannel;
        int partitionRequestInitialBackoff;
        int partitionRequestMaxBackoff;
        int floatingNetworkbuffersPerGate;
        int segmentSize;
        int numberofSegmentsGlobal;

        int sortShuffleMinBuffers;
        int sortShuffleMinParallelism;

};

}


#endif //SHUFFLEENVIRONMENTCONTEXT_H
/**
*  final ShuffleEnvironmentContext shuffleEnvironmentContext =
                new ShuffleEnvironmentContext(
                        taskManagerServicesConfiguration.getConfiguration(),
                        taskManagerServicesConfiguration.getResourceID(),
                        taskManagerServicesConfiguration.getNetworkMemorySize(),
                        taskManagerServicesConfiguration.isLocalCommunicationOnly(),
                        taskManagerServicesConfiguration.getBindAddress(),
                        taskEventDispatcher,
                        taskManagerMetricGroup,
                        ioExecutor);

private final Configuration configuration;
    private final ResourceID taskExecutorResourceId;
    private final MemorySize networkMemorySize;
    private final boolean localCommunicationOnly;
    private final InetAddress hostAddress;
    private final TaskEventPublisher eventPublisher;
    private final MetricGroup parentMetricGroup;
 **/