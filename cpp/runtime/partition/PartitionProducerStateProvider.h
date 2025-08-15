/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_PARTITIONPRODUCERSTATEPROVIDER_H
#define OMNISTREAM_PARTITIONPRODUCERSTATEPROVIDER_H

#include <memory>
#include <string>
#include <sstream>
#include <stdexcept>

#include <execution/ExecutionState.h>
#include <executiongraph/common/IntermediateDataSetIDPOD.h>
#include <executiongraph/descriptor/ResultPartitionIDPOD.h>
#include <utils/exception/Throwable.h>

namespace omnistream {


class PartitionProducerStateProvider {
public:
    class ResponseHandle {
    public:
        virtual ExecutionState getConsumerExecutionState() = 0;

        // TBD future
       // virtual Either<ExecutionState, Throwable> getProducerExecutionState() = 0;
        virtual void cancelConsumption() = 0;
        virtual void failConsumption(std::shared_ptr<Throwable> cause) = 0;
        virtual ~ResponseHandle() = default;
        virtual std::string toString() const = 0;
    };

    class Consumer {
    public:
        virtual void accept(std::shared_ptr<ResponseHandle> responseHandle) = 0;
        virtual ~Consumer() = default;
        virtual std::string toString() const = 0;
    };

    virtual void requestPartitionProducerState(
        std::shared_ptr<IntermediateDataSetIDPOD> intermediateDataSetId,
        std::shared_ptr<ResultPartitionIDPOD> resultPartitionId,
        std::shared_ptr<Consumer> responseConsumer) = 0;

    virtual ~PartitionProducerStateProvider() = default;
    virtual std::string toString() const = 0;
};

} // namespace omnistream

#endif // OMNISTREAM_PARTITIONPRODUCERSTATEPROVIDER_H