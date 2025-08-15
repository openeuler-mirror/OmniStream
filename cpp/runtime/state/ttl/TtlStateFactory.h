/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_TTLSTATEFACTORY_H
#define FLINK_TNEL_TTLSTATEFACTORY_H
#include "core/typeutils/TypeSerializer.h"
#include "core/api/common/state/StateDescriptor.h"
#include "runtime/state/KeyedStateBackend.h"

template <typename K, typename N, typename S, typename V>
class TtlStateFactory
{
public:
    // Function def does not match java, simplified to be passed to next class
    // stateBackend input should be KeyedStateBackend instead
    static S *createStateAndWrapWithTtlIfEnabled(TypeSerializer *namespaceSerialzer, StateDescriptor *stateDesc, KeyedStateBackend<K> *stateBackend)
    {
        return (S *) stateBackend->createOrUpdateInternalState(namespaceSerialzer, stateDesc);
    };
};

#endif // FLINK_TNEL_TTLSTATEFACTORY_H
