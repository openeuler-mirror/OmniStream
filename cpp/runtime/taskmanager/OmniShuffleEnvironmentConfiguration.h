/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/8/25.
//

#ifndef OMNISHUFFLEENVIRONMENTCONFIGURATION_H
#define OMNISHUFFLEENVIRONMENTCONFIGURATION_H
#include <executiongraph/descriptor/ResourceIDPOD.h>


namespace omnistream {
    class OmniShuffleEnvironmentConfiguration {
    public:
            // Default constructor
            OmniShuffleEnvironmentConfiguration() : numNetworkBuffers(0),
                                                    networkBufferSize(0),
                                                    requestSegmentsTimeoutMillis(0),
                                                    networkBuffersPerChannel(0),
                                                    partitionRequestMaxBackoff(0),
                                                    sortShuffleMinBuffers(0),
                                                    sortShuffleMinParallelism(0){
            }

            // Full argument constructor
            OmniShuffleEnvironmentConfiguration(int numNetworkBuffers,
                                                int networkBufferSize,
                                                long requestSegmentsTimeoutMillis,
                                                int networkBuffersPerChannel,
                                                int partitionRequestInitialBackoff,
                                                int partitionRequestMaxBackoff,
                                                int floatingNetworkBuffersPerGate,
                                                int sortShuffleMinBuffers,
                                                int sortShuffleMinParallelism)
                    : numNetworkBuffers(numNetworkBuffers), networkBufferSize(networkBufferSize),
                      requestSegmentsTimeoutMillis(requestSegmentsTimeoutMillis),
                      networkBuffersPerChannel(networkBuffersPerChannel),
                      partitionRequestMaxBackoff(partitionRequestMaxBackoff),
                      partitionRequestInitialBackoff(partitionRequestInitialBackoff),
                      floatingNetworkBuffersPerGate(floatingNetworkBuffersPerGate),
                    sortShuffleMinBuffers(sortShuffleMinBuffers),
                    sortShuffleMinParallelism(sortShuffleMinParallelism){
            }

            // Copy constructor
            OmniShuffleEnvironmentConfiguration(const OmniShuffleEnvironmentConfiguration &other)
                : numNetworkBuffers(other.numNetworkBuffers), networkBufferSize(other.networkBufferSize),
                  requestSegmentsTimeoutMillis(other.requestSegmentsTimeoutMillis),
                  networkBuffersPerChannel(other.networkBuffersPerChannel),
                  partitionRequestMaxBackoff(other.partitionRequestMaxBackoff),
                  partitionRequestInitialBackoff(other.partitionRequestInitialBackoff),
                  floatingNetworkBuffersPerGate(other.floatingNetworkBuffersPerGate),
                  sortShuffleMinBuffers(other.sortShuffleMinBuffers),
                  sortShuffleMinParallelism(other.sortShuffleMinParallelism){
            }

            // Getters
            int getNumNetworkBuffers() const { return numNetworkBuffers; }
            int getNetworkBufferSize() const { return networkBufferSize; }
            long getRequestSegmentsTimeoutMillis() const { return requestSegmentsTimeoutMillis; }
            int getNetworkBuffersPerChannel() const {
                return networkBuffersPerChannel;
            }

            int getPartitionRequestMaxBackoff() const {
                return partitionRequestMaxBackoff;
            }
            int getFloatingNetworkBuffersPerGate() const {
                return floatingNetworkBuffersPerGate;
            }

            int getPartitionRequestInitialBackoff() const {
                return partitionRequestInitialBackoff;
            }

            int GetsortShuffleMinBuffers() const
            {
                return sortShuffleMinBuffers;
            }

            int GetsortShuffleMinParallelism() const
            {
                return sortShuffleMinParallelism;
            }
            // Setters
            void setNumNetworkBuffers(int numNetworkBuffers) { this->numNetworkBuffers = numNetworkBuffers; }
            void setNetworkBufferSize(int networkBufferSize) { this->networkBufferSize = networkBufferSize; }

            void setRequestSegmentsTimeoutMillis(long requestSegmentsTimeoutMillis) {
                this->requestSegmentsTimeoutMillis = requestSegmentsTimeoutMillis;
            }
            void setNetworkBuffersPerChannel(int networkBuffersPerChannel) {
                this->networkBuffersPerChannel = networkBuffersPerChannel;
            }
            void setPartitionRequestMaxBackoff(int partitionRequestMaxBackoff) {
                this->partitionRequestMaxBackoff = partitionRequestMaxBackoff;
            }

            void setPartitionRequestInitialBackoff(int partitionRequestInitialBackoff) {
                this->partitionRequestInitialBackoff = partitionRequestInitialBackoff;
            }

            void setFloatingNetworkBuffersPerGate(int floatingNetworkBuffersPerGate) {
                this->floatingNetworkBuffersPerGate=floatingNetworkBuffersPerGate;
            }

            void SetsortShuffleMinParallelism(int newsortShuffleMinParallelism)
            {
                this->sortShuffleMinParallelism = newsortShuffleMinParallelism;
            }

            void SetsortShuffleMinBuffers(int newsortShuffleMinBuffers)
            {
                this->sortShuffleMinBuffers = newsortShuffleMinBuffers;
            }

            // toString method
            std::string toString() const {
                return "OmniShuffleEnvironmentConfiguration{ numNetworkBuffers=" + std::to_string(numNetworkBuffers) +
                       ", networkBufferSize=" + std::to_string(networkBufferSize) +
                       ", requestSegmentsTimeoutMillis=" + std::to_string(requestSegmentsTimeoutMillis) +
                        ", networkBuffersPerChannel=" + std::to_string(networkBuffersPerChannel) +
                        ", partitionRequestInitialBackoff=" + std::to_string(partitionRequestInitialBackoff) +
                        ", partitionRequestMaxBackoff=" + std::to_string(partitionRequestMaxBackoff) +
                       "}";
            };

            static std::shared_ptr<OmniShuffleEnvironmentConfiguration> fromConfiguration(
                int numNetworkBuffers, int networkBufferSize,
                long requestSegmentsTimeoutMillis, int networkBuffersPerChannel,
                int partitionRequestInitialBackoff, int partitionRequestMaxBackoff,
                int floatingNetworkBuffersPerGate,
                int sortShuffleMinBuffers, int sortShuffleMinParallelism);
    private:
            int numNetworkBuffers;
            int networkBufferSize;
            long requestSegmentsTimeoutMillis;
            int networkBuffersPerChannel;
            int partitionRequestMaxBackoff;
            int partitionRequestInitialBackoff;
            int floatingNetworkBuffersPerGate;
            int sortShuffleMinBuffers;
            int sortShuffleMinParallelism;
    };
}


#endif //OMNISHUFFLEENVIRONMENTCONFIGURATION_H
/***
 *
*        NettyShuffleEnvironmentConfiguration networkConfig =
                NettyShuffleEnvironmentConfiguration.fromConfiguration(
                        shuffleEnvironmentContext.getConfiguration(),
                        shuffleEnvironmentContext.getNetworkMemorySize(),
                        shuffleEnvironmentContext.isLocalCommunicationOnly(),
                        shuffleEnvironmentContext.getHostAddress());
        return createNettyShuffleEnvironment(
                networkConfig,
                shuffleEnvironmentContext.getTaskExecutorResourceId(),
                shuffleEnvironmentContext.getEventPublisher(),
                shuffleEnvironmentContext.getParentMetricGroup(),
                shuffleEnvironmentContext.getIoExecutor());
    }
 */
