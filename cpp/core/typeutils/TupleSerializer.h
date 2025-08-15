/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Tuple Serializer for DataStream
 */
#ifndef OMNISTREAM_TUPLESERIALIZER_H
#define OMNISTREAM_TUPLESERIALIZER_H

#include <nlohmann/json.hpp>
#include "TypeSerializer.h"
#include "basictypes/Tuple2.h"
#include "typeutils/LongSerializer.h"
#include "typeutils/StringSerializer.h"

class Tuple2Serializer : public TypeSerializerSingleton {
public:
    explicit Tuple2Serializer(nlohmann::json &type)
    {
        createSerializer(type);
    };
    Tuple2Serializer() = default;
    explicit Tuple2Serializer(BackendDataType firstType, BackendDataType secondType)
    {
        createSerializer(firstType, secondType);
    };

    ~Tuple2Serializer()
    {
        fieldSerializers.clear();
        // delete buffer;
    }

    void* deserialize(DataInputView& source) override;

    void serialize(void* record, DataOutputSerializer& target) override;

    void deserialize(Object *buffer, DataInputView &source) override;

    void serialize(Object *buffer, DataOutputSerializer &target) override;

    const char *getName() const override
    {
        return "TupleSerializer";
    }

    Object* GetBuffer() override;

    BackendDataType getBackendId() const override;

    static Tuple2Serializer *STRING_LONG_INSTANCE;

    class Tuple2SerializerCleaner {
    public:
        ~Tuple2SerializerCleaner()
        {
            if (STRING_LONG_INSTANCE) {
                delete STRING_LONG_INSTANCE;
                STRING_LONG_INSTANCE = nullptr;
            }
        }
    };

    static Tuple2SerializerCleaner cleaner;

    TypeSerializer* getTypeSerializer(BackendDataType typeId);

    void createSerializer(BackendDataType firstType, BackendDataType secondType);

    void createSerializer(nlohmann::json &type);

private:
    std::vector<TypeSerializer *> fieldSerializers;
    uint16_t arity = 0;
};

#endif //OMNISTREAM_TUPLESERIALIZER_H
