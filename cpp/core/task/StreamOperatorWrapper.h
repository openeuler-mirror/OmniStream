/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/15/24.
//

#ifndef FLINK_TNEL_STREAMOPERATORWRAPPER_H
#define FLINK_TNEL_STREAMOPERATORWRAPPER_H

#include "../operators/StreamOperator.h"

class StreamOperatorWrapper {
public:
    StreamOperatorWrapper(StreamOperator * op, bool isHead):wrapped_(op),isHead_(isHead){
        previous_=nullptr;
        next_=nullptr;
    }
    ~StreamOperatorWrapper() {
        delete wrapped_;
        delete next_;
    }
    void setNext(StreamOperatorWrapper* next){
        next_=next;
    }
    void setPrevious(StreamOperatorWrapper* prev){
        previous_=prev;
    }
    void setAsHead(){
        isHead_= true;
    }
    StreamOperator* getStreamOperator(){
        return wrapped_;
    }
    StreamOperatorWrapper* getPrevious(){
        return previous_;
    }
    StreamOperatorWrapper* getNext(){
        return next_;
    }


private:
    StreamOperator* wrapped_;
    StreamOperatorWrapper* previous_;
    StreamOperatorWrapper* next_;
    bool isHead_;

};

class ReadIterator {
public:
    ReadIterator(StreamOperatorWrapper* first, bool reverse)
        : reverse(reverse), current(first) {}

    bool hasNext() const {
        return this->current != nullptr;
    }

    StreamOperatorWrapper* next() {
        if (hasNext()) {
            StreamOperatorWrapper* next = current;
            current = reverse ? current->getPrevious() : current->getNext();
            return next;
        }
        throw std::invalid_argument("no such element");
    }

private:
    bool reverse;
    StreamOperatorWrapper* current;

};


#endif //FLINK_TNEL_STREAMOPERATORWRAPPER_H
