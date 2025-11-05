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
#ifndef FLINK_TNEL_TIMERSERIALIZER_H
#define FLINK_TNEL_TIMERSERIALIZER_H
#include "core/typeutils/TypeSerializer.h"
#include "streaming/api/operators/TimerHeapInternalTimer.h"

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
