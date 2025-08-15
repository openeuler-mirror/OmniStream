/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: String Serializer for DataStream
 */
#ifndef FLINK_TNEL_STRINGSERIALIZER_H
#define FLINK_TNEL_STRINGSERIALIZER_H

#include <memory>

#include "TypeSerializerSingleton.h"

class StringSerializer : public TypeSerializerSingleton {
public:
    StringSerializer() {};
    void *deserialize(DataInputView &source) override;

    void serialize(void *record, DataOutputSerializer &target) override;

    void deserialize(Object *buffer, DataInputView &source) override;

    void serialize(Object *buffer, DataOutputSerializer &target) override;

    Object* GetBuffer() override;
    
    BackendDataType getBackendId() const override
    {
        return BackendDataType::VARCHAR_BK;
    }

    static StringSerializer* INSTANCE;

    class StringSerializerCleaner {
    public:
        ~StringSerializerCleaner()
        {
            if (INSTANCE) {
                delete INSTANCE;
                INSTANCE = nullptr;
            }
        }
    };

    const char *getName() const override
    {
        return "StringSerializer";
    }

    static StringSerializerCleaner cleaner;
};
#endif //FLINK_TNEL_STRINGSERIALIZER_H
