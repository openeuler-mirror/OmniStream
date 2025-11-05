/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef OMNISTREAM_OPERATORSNAPSHOTFUTURES
#define OMNISTREAM_OPERATORSNAPSHOTFUTURES

#include <future>
#include <memory>
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/OperatorStateHandle.h"
#include "runtime/checkpoint/StateObjectCollection.h"
#include "runtime/state/InputChannelStateHandle.h"
#include "runtime/state/ResultSubpartitionStateHandle.h"

class OperatorSnapshotFutures {
public:
    explicit OperatorSnapshotFutures()
    {
        keyedStateManagedFuture = nullptr;
        keyedStateRawFuture = nullptr;
        operatorStateManagedFuture = nullptr;
        operatorStateRawFuture = nullptr;

        // Initialize input channel state future with completed future
        auto inputPromise =
                std::make_shared<std::promise<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>>();
        inputPromise->set_value(SnapshotResult<StateObjectCollection<InputChannelStateHandle>>(nullptr, nullptr));
        inputChannelStateFuture = std::make_shared<std::future<SnapshotResult
                <StateObjectCollection<InputChannelStateHandle>>>>(inputPromise->get_future());

        // Initialize result subpartition state future with completed future
        auto resultPromise = std::make_shared<std::promise<SnapshotResult
                <StateObjectCollection<ResultSubpartitionStateHandle>>>>();
        resultPromise->set_value(
            SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>(nullptr, nullptr));
        resultSubpartitionStateFuture = std::make_shared<std::future<SnapshotResult<
                StateObjectCollection<ResultSubpartitionStateHandle>>>>(resultPromise->get_future());
    };

    OperatorSnapshotFutures(
        std::shared_ptr<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>> keyedStateManagedFuture,
        std::shared_ptr<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>> keyedStateRawFuture,
        std::shared_ptr<std::packaged_task<SnapshotResult<OperatorStateHandle>>> operatorStateManagedFuture,
        std::shared_ptr<std::packaged_task<SnapshotResult<OperatorStateHandle>>> operatorStateRawFuture,
        std::shared_ptr<std::future<SnapshotResult<StateObjectCollection
            <InputChannelStateHandle>>>> inputChannelStateFuture,
        std::shared_ptr<std::future<SnapshotResult<StateObjectCollection
            <ResultSubpartitionStateHandle>>>> resultSubpartitionStateFuture)
        : keyedStateManagedFuture(keyedStateManagedFuture),
          keyedStateRawFuture(keyedStateRawFuture),
          operatorStateManagedFuture(operatorStateManagedFuture),
          operatorStateRawFuture(operatorStateRawFuture),
          inputChannelStateFuture(inputChannelStateFuture),
          resultSubpartitionStateFuture(resultSubpartitionStateFuture) {}

    std::shared_ptr<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>> getKeyedStateManagedFuture() const
    {
        return keyedStateManagedFuture;
    }

    void setKeyedStateManagedFuture(std::shared_ptr<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>> future)
    {
        keyedStateManagedFuture = future;
    }

    std::shared_ptr<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>> getKeyedStateRawFuture() const
    {
        return keyedStateRawFuture;
    }

    void setKeyedStateRawFuture(std::shared_ptr<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>> future)
    {
        keyedStateRawFuture = future;
    }

    std::shared_ptr<std::packaged_task<SnapshotResult<OperatorStateHandle>>> getOperatorStateManagedFuture() const
    {
        return operatorStateManagedFuture;
    }

    void setOperatorStateManagedFuture(std::shared_ptr<std::packaged_task<SnapshotResult<OperatorStateHandle>>> future)
    {
        operatorStateManagedFuture = future;
    }

    std::shared_ptr<std::packaged_task<SnapshotResult<OperatorStateHandle>>> getOperatorStateRawFuture() const
    {
        return operatorStateRawFuture;
    }

    void setOperatorStateRawFuture(std::shared_ptr<std::packaged_task<SnapshotResult<OperatorStateHandle>>> future)
    {
        operatorStateRawFuture = future;
    }

    std::shared_ptr<std::future<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>>
        getInputChannelStateFuture() const
    {
        return inputChannelStateFuture;
    }

    void setInputChannelStateFuture(std::shared_ptr<std::future<
        SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>> future)
    {
        inputChannelStateFuture = future;
    }

    std::shared_ptr<std::future<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>>
        getResultSubpartitionStateFuture() const
    {
        return resultSubpartitionStateFuture;
    }

    void setResultSubpartitionStateFuture(std::shared_ptr<std::future<SnapshotResult
        <StateObjectCollection<ResultSubpartitionStateHandle>>>> future)
    {
        resultSubpartitionStateFuture = future;
    }

    std::pair<long, long> cancel()
    {
        // TTODO
        return std::make_pair(0, 0);
    }

private:
    std::shared_ptr<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>> keyedStateManagedFuture;
    std::shared_ptr<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>> keyedStateRawFuture;
    std::shared_ptr<std::packaged_task<SnapshotResult<OperatorStateHandle>>> operatorStateManagedFuture;
    std::shared_ptr<std::packaged_task<SnapshotResult<OperatorStateHandle>>> operatorStateRawFuture;
    std::shared_ptr<std::future<SnapshotResult<StateObjectCollection<InputChannelStateHandle>>>>
        inputChannelStateFuture;
    std::shared_ptr<std::future<SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>>>>
        resultSubpartitionStateFuture;
};

#endif // OMNISTREAM_OPERATORSNAPSHOTFUTURES