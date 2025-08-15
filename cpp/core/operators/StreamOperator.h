/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_STREAMOPERATOR_H
#define FLINK_TNEL_STREAMOPERATOR_H
#include "core/operators/StreamTaskStateInitializerImpl.h"
#include "core/operators/Output.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"


class StreamOperator {
public:

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
};


#endif  //FLINK_TNEL_STREAMOPERATOR_H
