/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_TIMERSERIALIZER_H
#define FLINK_TNEL_TIMERSERIALIZER_H
#include "core/typeutils/TypeSerializer.h"
#include "TimerHeapInternalTimer.h"

template <typename K, typename N>
class TimerSerializer : public TypeSerializer {
public:
    TimerSerializer(TypeSerializer *keySerializer,
                    TypeSerializer *namespaceSerializer) : keySerializer(keySerializer),
                                                           namespaceSerializer(namespaceSerializer) {};

    void *deserialize(DataInputView &source) override { NOT_IMPL_EXCEPTION; };
    void serialize(void *record, DataOutputSerializer &target) override { NOT_IMPL_EXCEPTION; };
    BackendDataType getBackendId() const override { return BackendDataType::INVALID_BK;};
private:
    TypeSerializer *keySerializer;
    TypeSerializer *namespaceSerializer;
};

#endif // FLINK_TNEL_TIMERSERIALIZER_H
