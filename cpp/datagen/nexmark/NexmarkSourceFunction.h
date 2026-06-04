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

#ifndef OMNISTREAM_NEXMARKSOURCEFUNCTION_H
#define OMNISTREAM_NEXMARKSOURCEFUNCTION_H

#include <array>
#include <list>
#include <chrono>
#include <memory>
#include <vector>
#include <exception>
#include "core/api/common/state/ListStateDescriptor.h"
#include "core/typeinfo/TypeInformation.h"
#include "core/typeutils/LongSerializer.h"
#include "functions/SourceFunction.h"
#include "functions/AbstractRichFunction.h"
#include "GeneratorConfig.h"
#include "generator/NexmarkGenerator.h"
#include "../source/EventDeserializer.h"
#include "table/runtime/operators/source/InputFormatSourceFunction.h"
#include "functions/Configuration.h"
#include "streaming/api/checkpoint/CheckpointedFunction.h"
#include "core/api/common/state/ListState.h"
#include "runtime/state/DefaultOperatorStateBackend.h"

template<typename K>
class NexmarkSourceFunction : public SourceFunction<K>, public AbstractRichFunction, public CheckpointedFunction {
    // Configuration for generator to use when reading synthetic events. May be split.
    GeneratorConfig config;

    EventDeserializer* deserializer;

    TypeInformation* resultType;

    // Transient generator pointer.
    std::unique_ptr<NexmarkGenerator> generator;

    // The number of elements emitted already.
    volatile long numElementsEmitted;

    // Flag to make the source cancelable.
    volatile bool isRunning;

    // Transient checkpointed state.
    std::shared_ptr<ListState<long>> checkpointedState;

public:
    NexmarkSourceFunction(const GeneratorConfig& config,
                          EventDeserializer* deserializer,
                          TypeInformation* resultType)
        : config(config),
          deserializer(deserializer),
          resultType(resultType),
          generator(nullptr),
          numElementsEmitted(0),
          isRunning(true) {}

    // Overriding open method.
    void open(const Configuration& parameters) override {
        AbstractRichFunction::open(parameters);
        this->generator.reset(new NexmarkGenerator(getSubGeneratorConfig()));
    }

    // Private method to get sub-generator config.
    GeneratorConfig getSubGeneratorConfig() {
        int parallelism = this->getRuntimeContext()->getNumberOfParallelSubtasks();
        int taskId = this->getRuntimeContext()->getIndexOfThisSubtask();
        std::vector<GeneratorConfig> splits = config.split(parallelism);
        return splits.at(taskId);
    }

    void initializeState() {
        // checkpoint stuff
    }

    // Overriding run method.
    void run(SourceContext* ctx) override {
        while (isRunning && generator->hasNext()) {
            long now = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
            NexmarkGenerator::NextEvent nextEvent = generator->nextEvent();
            if (nextEvent.wallclockTimestamp > now) {
                // sleep until wall clock less than current timestamp.
                std::this_thread::sleep_for(std::chrono::milliseconds(nextEvent.wallclockTimestamp - now));
            }

            auto next = deserializer->deserialize(std::move(nextEvent.event));
            {
                ctx->getCheckpointLock()->mutex.lock();
                numElementsEmitted = generator->getEventsCountSoFar();
                // Only do output when a batch is prepared
                if (next) {
                    try {
                        ctx->collect(next->getValue());
                    } catch (const std::exception &e) {
                        std::cerr << "Exception during collect: " << e.what() << std::endl;
                    }
                }
                ctx->getCheckpointLock()->mutex.unlock();
            }
        }
    }

    // Overriding cancel method.
    void cancel() override {
        isRunning = false;
    }

    // Overriding close method.
    void close() override {
        AbstractRichFunction::close();
    }

    // Overriding getProducedType method.
    TypeInformation* getProducedType() {
        return resultType;
    }
    
    void snapshotState(StateSnapshotContextSynchronousImpl *context) override {
        this->checkpointedState->clear();
        this->checkpointedState->add(const_cast<long&>(numElementsEmitted));
    }

    void initializeState(StateInitializationContextImpl *context) override {
        std::string stateName = "elements-count-state";
        auto *listStateDescriptor = new ListStateDescriptor<long>(stateName, new LongSerializer());
        auto *stateBackend = static_cast<DefaultOperatorStateBackend*>(context->getOperatorStateBackend());
        this->checkpointedState = stateBackend->template getListState<long>(listStateDescriptor);

        if (context->isRestored()) {
            std::vector<long> retrievedStates;
            for (auto const& entry : *this->checkpointedState->get()) {
                retrievedStates.push_back(entry);
            }
            if (retrievedStates.size() != 1) {
                throw std::runtime_error("NexmarkSourceFunction retrieve invalid state.");
            }
            auto numElementToSkip = retrievedStates[0];
            INFO_RELEASE("NexmarkSourceFunction::initializeState, numElementToSkip: " << numElementToSkip);
            this->generator.reset(new NexmarkGenerator(getSubGeneratorConfig(), numElementToSkip, 0));
        }
    }
};


#endif // OMNISTREAM_NEXMARKSOURCEFUNCTION_H
