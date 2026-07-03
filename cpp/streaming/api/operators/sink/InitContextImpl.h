#ifndef OMNIFLINK_INIT_CONTEXT_IMPL_H
#define OMNIFLINK_INIT_CONTEXT_IMPL_H

#include <optional>
#include <cstdint>

#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "streaming/runtime/tasks/ProcessingTimeServiceImpl.h"

template <typename K>
class InitContextImpl {
private:
    ProcessingTimeServiceImpl* processingTimeService;
    std::optional<uint64_t> restoredCheckpointId;
    StreamingRuntimeContext<K>* runtimeContext;

public:
    InitContextImpl(
        StreamingRuntimeContext<K>* runtimeContext,
        ProcessingTimeServiceImpl* processingTimeService,
        std::optional<uint64_t> restoredCheckpointId)
        : runtimeContext(runtimeContext),
          processingTimeService(processingTimeService),
          restoredCheckpointId(restoredCheckpointId)
    {
    }

    int getNumberOfParallelSubtasks()
    {
        return runtimeContext->getNumberOfParallelSubtasks();
    }

    ProcessingTimeServiceImpl* getProcessingTimeService()
    {
        return processingTimeService;
    }

    int getSubtaskId()
    {
        return runtimeContext->getIndexOfThisSubtask();
    }

    std::optional<uint64_t> getRestoredCheckpointId()
    {
        return restoredCheckpointId;
    }
};

#endif // OMNIFLINK_INIT_CONTEXT_IMPL_H
