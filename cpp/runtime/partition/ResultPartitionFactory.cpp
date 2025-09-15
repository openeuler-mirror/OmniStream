/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "ResultPartitionFactory.h"


#include <buffer/NetworkObjectBufferPool.h>

#include <taskmanager/OmniShuffleEnvironmentConfiguration.h>
#include "PipelinedResultPartition.h"
#include "PipelinedSubpartition.h"
#include "ResultPartitionType.h"
namespace omnistream  {

    ResultPartitionFactory::ResultPartitionFactory(
        std::shared_ptr<ResultPartitionManager> partitionManager,
        std::shared_ptr<ObjectBufferPoolFactory> bufferPoolFactory,
        int networkBufferSize) : partitionManager(partitionManager),
                                 bufferPoolFactory(bufferPoolFactory), networkBufferSize(networkBufferSize)
    {
    }


    std::shared_ptr<ResultPartition> ResultPartitionFactory::create(
        const std::string& taskNameWithSubtaskAndId,
        int partitionIndex,
        const ResultPartitionDeploymentDescriptorPOD& desc,
        std::shared_ptr<OmniShuffleEnvironmentConfiguration> config)
    {
        return create(

            taskNameWithSubtaskAndId,
            partitionIndex,
            desc.getShuffleDescriptor().getResultPartitionID(),
            desc.getPartitionType(),
            desc.getNumberOfSubpartitions(),
            desc.getMaxParallelism(),
            createBufferPoolFactory(desc.getNumberOfSubpartitions(), desc.getPartitionType(),config),
            config);
    }

    std::shared_ptr<ResultPartition> ResultPartitionFactory::create(
        const std::string& taskNameWithSubtaskAndId,
        int partitionIndex,
        const ResultPartitionIDPOD& id,
        int resultPartitionType,
        int numberOfSubpartitions,
        int maxParallelism,
        std::shared_ptr<Supplier<ObjectBufferPool>> bufferPoolFactory,
        std::shared_ptr<OmniShuffleEnvironmentConfiguration> config)
    {
        LOG_PART("Inside real partition creation.")
        LOG_PART(" resultPartitionType  " << resultPartitionType  << " name " << ResultPartitionType::getNameByType(resultPartitionType))

        if (resultPartitionType == ResultPartitionType::PIPELINED  || resultPartitionType == ResultPartitionType::PIPELINED_BOUNDED)
        {   auto pipelinedPartition = std::make_shared<PipelinedResultPartition>(
                taskNameWithSubtaskAndId,
                partitionIndex,
                id,
                resultPartitionType,
                numberOfSubpartitions,
                maxParallelism,
                partitionManager,
                bufferPoolFactory);

            std::vector<std::shared_ptr<ResultSubpartition>> resultPartitionns;
            LOG_PART("Just before sub partition creation. numberOfSubpartitions is  "  << std::to_string(numberOfSubpartitions))
            for (auto i = 0; i < numberOfSubpartitions; i++)
            {
                LOG_PART("Inside sub partition creation. index is  "  << std::to_string(i))
                //todo: configuredNetworkBuffersPerChannel and NetworkBuffersPerChannel are identical
                int configuredNetworkBuffersPerChannel = config->getNetworkBuffersPerChannel();
                auto subPartition  =   std::make_shared<PipelinedSubpartition>(i, configuredNetworkBuffersPerChannel, pipelinedPartition);
                resultPartitionns.push_back(subPartition);
            }
            pipelinedPartition->setSubpartitions(resultPartitionns);
            return pipelinedPartition;
        } else
        {
            THROW_LOGIC_EXCEPTION("only support pipelined result partition")
        }
    }


    class ObjectBufferPoolFactoryLambda : public Supplier<ObjectBufferPool>
    {
      private:
        std::shared_ptr<ObjectBufferPoolFactory> factory_;
        int  Left_numRequiredBuffers;
        int  Right_maxUsedBuffers;
        int  numberOfSubpartitions;
        int  resultPartitionType;
        std::shared_ptr<OmniShuffleEnvironmentConfiguration> config;

    public:
        ObjectBufferPoolFactoryLambda(const std::shared_ptr<ObjectBufferPoolFactory>& factory,
            int leftNumRequiredBuffers, int rightMaxUsedBuffers, int numberOfSubpartitions, int resultPartitionType,std::shared_ptr<OmniShuffleEnvironmentConfiguration> config)
            : factory_(factory),
              Left_numRequiredBuffers(leftNumRequiredBuffers),
              Right_maxUsedBuffers(rightMaxUsedBuffers),
              numberOfSubpartitions(numberOfSubpartitions),
              resultPartitionType(resultPartitionType),
              config(config)
        {
        }

        std::shared_ptr<ObjectBufferPool> get() override
        {
            auto networkpool = std::static_pointer_cast<NetworkObjectBufferPool>(factory_);
            LOG("ObjectBufferPoolFactoryLambda get")
            return networkpool->createBufferPool(Left_numRequiredBuffers, Right_maxUsedBuffers, numberOfSubpartitions,
                config->getNetworkBuffersPerChannel());
            //todo: add config
              //FakerConfig::networkBuffersPerChannel);
        };
        std::string toString() const override { return {}; };
        ~ObjectBufferPoolFactoryLambda() override = default;
    };

    std::shared_ptr<Supplier<ObjectBufferPool>> ResultPartitionFactory::createBufferPoolFactory(int numberOfSubpartitions,
        int resultPartitionType,std::shared_ptr<OmniShuffleEnvironmentConfiguration> config)
    {
        LOG_PART("Beginning of createBufferPoolFactory")
        bool isSortShuffle = (resultPartitionType == ResultPartitionType::BLOCKING_PERSISTENT ||
             resultPartitionType==ResultPartitionType::BLOCKING) &&
             numberOfSubpartitions>=config->GetsortShuffleMinParallelism();
        int min_val = isSortShuffle? config->GetsortShuffleMinBuffers():numberOfSubpartitions+1;

        bool isbounded = (resultPartitionType == ResultPartitionType::PIPELINED_BOUNDED ||
             resultPartitionType == ResultPartitionType::PIPELINED_APPROXIMATE);

        int max_val = isbounded?
            (numberOfSubpartitions*config->getNetworkBuffersPerChannel()+config->getFloatingNetworkBuffersPerGate()):
            (isSortShuffle? std::max(min_val, 4*numberOfSubpartitions):INT_MAX);
        std::pair<int, int> pairs = std::make_pair(min_val, std::max(min_val, max_val));

        int leftNumRequiredBuffers = pairs.first;

        int rightMaxUsedBuffers = pairs.second;

        auto supplier = std::make_shared<ObjectBufferPoolFactoryLambda>(bufferPoolFactory,
            leftNumRequiredBuffers,
            rightMaxUsedBuffers,
            numberOfSubpartitions,
            resultPartitionType,
            config);
        /**
        auto supplier = std::make_shared<LambdaSupplier<ObjectBufferPool>>(  [=]()-> std::shared_ptr<ObjectBufferPool> {

             LOG_PART("Before createBufferPool")
             return bufferPoolFactory->createBufferPool(Left_numRequiredBuffers,
                 Right_maxUsedBuffers,
                 numberOfSubpartitions,
                 resultPartitionType);
        }
        );**/

        LOG_PART("end  of createBufferPoolFactory")
        return supplier;
    }

}

