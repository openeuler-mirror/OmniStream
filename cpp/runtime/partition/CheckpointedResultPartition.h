//
// Created by root on 12/30/25.
//

#ifndef OMNISTREAM_CHECKPOINTEDRESULTPARTITION_H
#define OMNISTREAM_CHECKPOINTEDRESULTPARTITION_H
#include "CheckpointedResultSubpartition.h"
namespace omnistream {
    class CheckpointedResultPartition {
    public:
        virtual std::shared_ptr<CheckpointedResultSubpartition> getCheckpointedSubpartition(int subpartitionIndex) = 0;

        virtual void finishReadRecoveredState(bool notifyAndBlockOnCompletion) = 0;
    };
}

#endif //OMNISTREAM_CHECKPOINTEDRESULTPARTITION_H
