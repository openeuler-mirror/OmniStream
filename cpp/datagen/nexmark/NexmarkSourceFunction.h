/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_NEXMARKSOURCEFUNCTION_H
#define OMNISTREAM_NEXMARKSOURCEFUNCTION_H

#include <array>
#include <list>
#include <chrono>
#include "core/api/ListState.h"
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

template<typename K>
class NexmarkSourceFunction : public SourceFunction<K>, public AbstractRichFunction {
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
    // ListState<long>* checkpointedState;

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
};


#endif // OMNISTREAM_NEXMARKSOURCEFUNCTION_H
