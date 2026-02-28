//
// Created by root on 12/30/25.
//

#ifndef OMNISTREAM_CHECKPOINTEDRESULTSUBPARTITION_H
#define OMNISTREAM_CHECKPOINTEDRESULTSUBPARTITION_H
#include "ResultSubpartitionInfoPOD.h"
#include "runtime/buffer/BufferBuilder.h"
namespace omnistream {
    class CheckpointedResultSubpartition {
    public:
        virtual const ResultSubpartitionInfoPOD &getSubpartitionInfo() = 0;

        virtual BufferBuilder *requestBufferBuilderBlocking() = 0;

        virtual void addRecovered(std::shared_ptr<BufferConsumer> bufferConsumer) = 0;

        virtual void finishReadRecoveredState(bool notifyAndBlockOnCompletion) = 0;
    };
}
#endif //OMNISTREAM_CHECKPOINTEDRESULTSUBPARTITION_H
