//
// Created by xichen on 2/22/25.
//
#include <gtest/gtest.h>
#include "datagen/nexmark/generator/NexmarkGenerator.h"
#include "datagen/nexmark/NexmarkSourceFunction.h"
#include "core/typeinfo/TypeInfoFactory.h"
#include "runtime/taskmanager/RuntimeEnvironment.h"
#include "core/operators/StreamingRuntimeContext.h"

class DummySourceContext : public SourceContext {
public:
    DummySourceContext(Object *lock) : lock(lock) {
    }

    void collect(void *element) override {
        reUseRecord = element;
    }

    void collectWithTimestamp(void *element, int64_t timestamp) override {
        // ignore the timestamp
        collect(element);
    }

    void emitWatermark(Watermark* mark) override {
        // do nothing
    }

    void markAsTemporarilyIdle() override {
        // do nothing
    }

    Object *getCheckpointLock() override {
        return lock;
    }

    void close() override {}

    void* reUseRecord;
    Object *lock;
};

TEST(SourceTest, NexmarkDataGeneratorTest) {
    int batchSize = 100;
    BatchEventDeserializer* eventDeserializer = new BatchEventDeserializer(batchSize);
    auto typeInfo = TypeInfoFactory::createTypeInfo("String", "TBD");

    // In NexmarkConfiguration, all values have been set to their default value
    NexmarkConfiguration nexmarkConfig;

    /*
     * const NexmarkConfiguration &configuration,
                int64_t baseTime,
                int64_t firstEventId,
                int64_t maxEventsOrZero,
                int64_t firstEventNumber
    */
    std::string baseTimeStr = "2025-02-22 00:00:00";
    GeneratorConfig config {nexmarkConfig, 1740182400000, 0, 100, 0};
    NexmarkSourceFunction<omnistream::VectorBatch> srcFunc {config, eventDeserializer, typeInfo};

    auto runtimeEnv = new RuntimeEnvironment(new TaskInfoImpl("SourceFunctionTest", 2, 1, 0));
    auto runtimeCtx = new StreamingRuntimeContext<int>();
    runtimeCtx->setEnvironment(runtimeEnv);
    srcFunc.setRuntimeContext(runtimeCtx);
    Configuration runConfig;
    srcFunc.open(runConfig);
    thread_local Object lock;
    DummySourceContext ctx(&lock);
    srcFunc.run(&ctx);
    auto output = reinterpret_cast<omnistream::VectorBatch*> (ctx.reUseRecord);
    EXPECT_EQ(output->GetRowCount(), batchSize);
}