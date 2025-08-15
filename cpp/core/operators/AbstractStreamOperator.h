/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_ABSTRACTSTREAMOPERATOR_H
#define FLINK_TNEL_ABSTRACTSTREAMOPERATOR_H

#include <type_traits>
#include <nlohmann/json.hpp>
#include "StreamOperator.h"
#include "AbstractStreamOperator.h"
#include "StreamOperatorStateHandler.h"
#include "Output.h"
#include "../streamrecord/StreamRecord.h"
#include "StreamingRuntimeContext.h"
#include "StreamTaskStateInitializerImpl.h"
#include "streaming/api/operators/ChainingStrategy.h"
#include "Input.h"
#include "data/binary/BinaryRowData.h"
#include "table/runtime/operators/InternalTimerServiceImpl.h"
#include "table/runtime/operators/InternalTimeServiceManager.h"
#include "KeyContext.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"
#include "core/api/common/eventtime/IndexedCombinedWatermarkStatus.h"
#include "table/typeutils/RowDataSerializer.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "runtime/tasks/OmniStreamTask.h"

// TODO: operator shouldn't be keyed, but the <K> is proporgated from backend to statestore to statehandler
template <typename K>
class AbstractStreamOperator : public StreamOperator, public KeyContext<K> {
public:
    void setDescription(nlohmann::json description)
    {
        desc = description;
    }

    AbstractStreamOperator()
    {
        this->runtimeContext = nullptr;
    }

    explicit AbstractStreamOperator(Output *output)
    {
        this->output = output;
        this->runtimeContext = nullptr;
    }

    virtual ~AbstractStreamOperator()
    {
        LOG("AbstractStreamOperator::~AbstractStreamOperator()");
        // delete output; ?
        delete stateHandler;
        delete runtimeContext;
    };

    void setup()
    {
        LOG("AbstractStreamOperator::setup()" << "new StreamingRuntimeContext<K>");
        // TODO: rethink about this nullptr!
        this->runtimeContext = new StreamingRuntimeContext<K>(nullptr, nullptr);
        constexpr int inputsCount = 2;
        combinedWatermark = new omnistream::IndexedCombinedWatermarkStatus(inputsCount);
        // Flink intialize stateKeySelector here
    }

    void SetOpName(std::string operatorName)
    {
        this->opName = operatorName;
    }

    std::string GetOpName()
    {
        return this->opName;
    }

    void setup(std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        this->setup();
        if (task != nullptr) {
            this->metrics = task->env()->taskMetricGroup();
        }
        // Flink intialize stateKeySelector here
    }

    std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics() override
    {
        LOG("AbstractStreamOperator GetMectrics")
        return this->metrics;
    }

    void setOutput(Output *output)
    {
        this->output = output;
    }

    void setCurrentKey(K key) override
    {
        stateHandler->setCurrentKey(key);
    };

    K getCurrentKey() override
    {
        return stateHandler->getCurrentKey();
    };

    void open() override{};

    void close() override{
        if (stateHandler != nullptr) {
            stateHandler->dispose();
        }
    };

    TypeSerializer *GetOperatorKeySerializer()
    {
        return new BinaryRowDataSerializer(1);
    };

    // KeySerializer should be retrieved from description.getStateKeySerializer(getUserCodeClassloader()),
    // but we're just passing it through this function for now
    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
    {
        LOG("abstractStreamOperator::initializeState")
        if (!canBeStreamOperator()) {
            keySerializer = GetOperatorKeySerializer();
        }
        StreamOperatorStateContextImpl<K> *context =
            initializer->streamOperatorStateContext<K>(keySerializer, this, processingTimeService);
        stateHandler = new StreamOperatorStateHandler<K>(context);
        auto stateStore = stateHandler->getKeyedStateStore();
        runtimeContext->setKeyedStateStore(stateStore);
        runtimeContext->setEnvironment(initializer->getEnvironment());
        timeServiceManager = context->getInternalTimeServiceManager();
    }
    StreamingRuntimeContext<K> *getRuntimeContext() const
    {
        return runtimeContext;
    }

    AbstractKeyedStateBackend<K> *getKeyedStateBackend() const
    {
        return stateHandler->getKeyedStateBackend();
    }

    std::string getTypeName() override
    {
        std::string typeName = "AbstractStreamOperator";
        typeName.append(__PRETTY_FUNCTION__);
        return typeName;
    }
    template <typename N>
    InternalTimerServiceImpl<K, N> *getInternalTimerService(
        std::string name, TypeSerializer *namespaceSerializer, Triggerable<K, N> *triggerable)
    {
        if (timeServiceManager == nullptr) {
            THROW_LOGIC_EXCEPTION("The timer service has not been initialized")
        }
        AbstractKeyedStateBackend<K> *keyedStateBackend = getKeyedStateBackend();
        if (keyedStateBackend == nullptr) {
            THROW_LOGIC_EXCEPTION("Timers can only be used on keyed operators")
        }
        return timeServiceManager->getInternalTimerService(
            name, keyedStateBackend->getKeySerializer(), namespaceSerializer, triggerable);
    }

    virtual void ProcessWatermark(Watermark* mark)
    {
        if (timeServiceManager != nullptr) {
            timeServiceManager->template advanceWatermark<VoidNamespace>(mark);
        }
        output->emitWatermark(mark);
    }

    void ProcessWatermark1(Watermark* mark)
    {
        ProcessWatermark(mark, 0);
    }

    void ProcessWatermark2(Watermark* mark)
    {
        ProcessWatermark(mark, 1);
    }
    void processWatermarkStatus(WatermarkStatus *watermarkStatus)
    {
        output->emitWatermarkStatus(watermarkStatus);
    }

    void setProcessingTimeService(ProcessingTimeService *service)
    {
        processingTimeService = service;
    };

    ProcessingTimeService *getProcessingTimeService()
    {
        return processingTimeService;
    };

protected:
    // own  and  own the backend through stateHandler
    StreamOperatorStateHandler<K> *stateHandler{};

    Output *output;
    // should not own the backend though runtimeContext
    StreamingRuntimeContext<K> *runtimeContext;
    ChainingStrategy chainingStrategy;
    nlohmann::json desc;
    InternalTimeServiceManager<K> *timeServiceManager = nullptr;
    std::shared_ptr<omnistream::TaskMetricGroup> metrics;
    std::string opName;
private:
    omnistream::IndexedCombinedWatermarkStatus* combinedWatermark;
    ProcessingTimeService *processingTimeService;

    void ProcessWatermark(Watermark* mark, int index)
    {
        LOG(">>>>>>>>>>")
        if (combinedWatermark->UpdateWatermark(index, mark->getTimestamp())) {
            this->ProcessWatermark(new Watermark(combinedWatermark->GetCombinedWatermark()));
        }
    }
};
#endif // FLINK_TNEL_ABSTRACTSTREAMOPERATOR_H
