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

#include "core/typeutils/TypeSerializerSingleton.h"
#include "streaming/api/operators/TimerHeapInternalTimer.h"
#include "basictypes/Class.h"


class TimerSerializer : public TypeSerializerSingleton {
public:
    TimerSerializer(TypeSerializer* keySerializer_, TypeSerializer* namespaceSerializer_, Class* keyClazz_, Class* namespaceClazz_);

    ~TimerSerializer() override;

    TimerHeapInternalTimer<Object*, Object*>* createInstance();

    void* deserialize(DataInputView& source) override { NOT_IMPL_EXCEPTION }

    void serialize(void* record, DataOutputSerializer& target) override { NOT_IMPL_EXCEPTION }

    void serialize(Object* buffer, DataOutputSerializer& target) override;

    void deserialize(Object* buffer, DataInputView& source) override;

    std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration() {
		// TODO impl build serializer snapshot
        NOT_IMPL_EXCEPTION
    }

    BackendDataType getBackendId() const override { return BackendDataType::OBJECT_BK; }

    const char* getName() const override { return "TimerSerializer"; }

	void setSubBufferReusable(bool bufferReusable_) override;

    Object* GetBuffer() override;

    std::string toJson() override {
        SerializerJsonInfo typeJson = {SerializerType::TIMER, "", keySerializer, nullptr, namespaceSerializer};
        return typeJson.toJson();
    };

private:
    TypeSerializer* keySerializer;
    TypeSerializer* namespaceSerializer;

    Class* keyClazz;
    Class* namespaceClazz;
};

#endif // FLINK_TNEL_TIMERSERIALIZER_H
