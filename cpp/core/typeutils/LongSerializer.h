/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Long Serializer for DataStream
 */
#ifndef FLINK_TNEL_LONGSERIALIZER_H
#define FLINK_TNEL_LONGSERIALIZER_H

#include "TypeSerializerSingleton.h"

class LongSerializer : public TypeSerializerSingleton {
public:
    LongSerializer();

    void *deserialize(DataInputView &source) override;
    void serialize(void *record, DataOutputSerializer &target) override;

    void deserialize(Object *buffer, DataInputView& source) override;
    void serialize(Object *buffer, DataOutputSerializer& target) override;
    Object* GetBuffer() override;

    static LongSerializer* INSTANCE;

    class LongSerializerCleaner {
    public:
        ~LongSerializerCleaner()
        {
            if (INSTANCE) {
                delete INSTANCE;
                INSTANCE = nullptr;
            }
        }
    };

    const char *getName() const override
    {
        return "LongSerializer";
    }

    // to delete static instance object
    static LongSerializerCleaner cleaner;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::BIGINT_BK;
    };
};

class IntSerializer : public TypeSerializerSingleton {
public:
    IntSerializer();

    void *deserialize(DataInputView &source) override;
    void serialize(void *record, DataOutputSerializer &target) override;

    void deserialize(Object *buffer, DataInputView& source) override;
    void serialize(Object *buffer, DataOutputSerializer& target) override;
    Object* GetBuffer() override;

    static IntSerializer* INSTANCE;

    class IntSerializerCleaner {
    public:
        ~IntSerializerCleaner()
        {
            if (INSTANCE) {
                delete INSTANCE;
                INSTANCE = nullptr;
            }
        }
    };

    const char *getName() const override
    {
        return "IntSerializer";
    }

    // to delete static instance object
    static IntSerializerCleaner cleaner;

    BackendDataType getBackendId() const override
    {
        return BackendDataType::INT_BK;
    };
};

#endif //FLINK_TNEL_LONGSERIALIZER_H
