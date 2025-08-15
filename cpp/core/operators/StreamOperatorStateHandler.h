/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_STREAMOPERATORSTATEHANDLER_H
#define FLINK_TNEL_STREAMOPERATORSTATEHANDLER_H
#include "runtime/state/AbstractKeyedStateBackend.h"
#include "StreamOperatorStateContext.h"
#include "runtime/state/DefaultKeyedStateStore.h"
#include "core/operators/StreamTaskStateInitializerImpl.h"
template<typename K>
class StreamOperatorStateHandler
{
public:
    StreamOperatorStateHandler(StreamOperatorStateContextImpl<K> *context) {
        this->keyedStateBackend = context->keyedStateBackend();
        if (keyedStateBackend != nullptr)
        {
            keyedStateStore = new DefaultKeyedStateStore<K>(keyedStateBackend);
        }
        else
        {
            keyedStateStore = nullptr;
        }
    };

    ~StreamOperatorStateHandler() {
        LOG("StreamOperatorStateHandler::~StreamOperatorStateHandler - delete keyedStateBackend");
        delete keyedStateBackend;
    }
    void setCurrentKey(K key) {
        keyedStateBackend->setCurrentKey(key);
    };
    K getCurrentKey() {
        return keyedStateBackend->getCurrentKey();
    };
    AbstractKeyedStateBackend<K> *getKeyedStateBackend() {
        return keyedStateBackend;
    };
    DefaultKeyedStateStore<K> *getKeyedStateStore() {
        return keyedStateStore;
    };

    void dispose() {
        if (keyedStateBackend != nullptr) {
            keyedStateBackend->dispose();
            delete keyedStateBackend;
            keyedStateBackend = nullptr;
        }
    }
private:
    // own backend
    AbstractKeyedStateBackend<K> *keyedStateBackend;

    DefaultKeyedStateStore<K> *keyedStateStore;
};

#endif // FLINK_TNEL_STREAMOPERATORSTATEHANDLER_H
