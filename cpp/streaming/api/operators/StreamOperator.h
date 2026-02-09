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

#ifndef FLINK_TNEL_STREAMOPERATOR_H
#define FLINK_TNEL_STREAMOPERATOR_H
#include "StreamTaskStateInitializerImpl.h"
#include "Output.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "runtime/state/CheckpointStreamFactory.h"
#include "OperatorSnapshotFutures.h"
#include "runtime/jobgraph/OperatorID.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/TaskStateSnapshotDeserializer.h"
#include "runtime/checkpoint/CheckpointListener.h"

class StreamOperator : public CheckpointListener {
public:
    StreamOperator()
    {
        // Give it a random ID
        operatorId = OperatorID();
    }
    virtual void open() { };
    virtual void close() { };
    virtual void setKeyContextElement(StreamRecord* record){};
    virtual void setKeyContextElement1(StreamRecord* record){};
    virtual void setKeyContextElement2(StreamRecord* record){};
    virtual void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) =0;
    virtual bool isSetKeyContextElement()
    {
        return false;
    }

    virtual bool isSetKeyContextElement1()
    {
        return false;
    }

    virtual bool isSetKeyContextElement2()
    {
        return false;
    }

    virtual bool canBeStreamOperator()
    {
        return false;
    }

    virtual void finish() { };

    // for debugging
    virtual std::string getTypeName() = 0;

    virtual ~StreamOperator()
    {
        LOG("StreamOperator::~StreamOperator()");
    }

    virtual std::shared_ptr<omnistream::TaskMetricGroup> GetMectrics()
    {
        LOG("Stream Operator GetMectrics")
        return nullptr;
    };

     virtual void PrepareSnapshotPreBarrier(long checkpointId)
     {
         LOG("prepareSnapshotPreBarrier " << checkpointId);
     };

    /**
     * Called to draw a state snapshot from the operator.
     *
     * @return a runnable future to the state handle that points to the snapshotted state. For
     *     synchronous implementations, the runnable might already be finished.
     * @throws Exception exception that happened during snapshotting.
     */
    virtual OperatorSnapshotFutures *SnapshotState(long checkpointId, long timestamp,
        CheckpointOptions *checkpointOptions, CheckpointStreamFactory* storageLocation,
        const std::shared_ptr<OmniTaskBridge>& bridge)
    {
        LOG("checkpointId " << checkpointId << " timestamp " << timestamp);
        return new OperatorSnapshotFutures();
    }

    virtual void notifyCheckpointComplete(long checkpointId)
    {
        LOG("checkpointID " << checkpointId)
    }

    virtual void NotifyCheckpointAborted(long checkpointId)
    {
        LOG("checkpointID " << checkpointId)
    }
    virtual void NotifyCheckpointSubsumed(long checkpointId)
    {
        LOG("checkpointID " << checkpointId)
    }
    virtual void SetOperatorID(const std::string& operatorId_)
    {
        this->operatorId = TaskStateSnapshotDeserializer::HexStringToOperatorId<OperatorID>(operatorId_);
    }
    virtual OperatorID GetOperatorID()
    {
        return operatorId;
    };
protected:
    OperatorID operatorId;
};


#endif
