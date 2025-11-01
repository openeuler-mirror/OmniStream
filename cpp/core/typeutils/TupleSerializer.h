/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * @Description: Tuple Serializer for DataStream
 */
#ifndef OMNISTREAM_TUPLESERIALIZER_H
#define OMNISTREAM_TUPLESERIALIZER_H

#include <nlohmann/json.hpp>
#include "TypeSerializer.h"
#include "basictypes/Tuple2.h"
#include "typeinfo/TypeInformation.h"
#include "typeutils/LongSerializer.h"
#include "typeutils/StringSerializer.h"

class Tuple2Serializer : public TypeSerializerSingleton {
public:
    explicit Tuple2Serializer(const std::vector<TypeInformation*> &types);
    explicit Tuple2Serializer(nlohmann::json &type);
    Tuple2Serializer();

    ~Tuple2Serializer()
    {
        for (int i = 0; i < arity; ++i) {
            delete fieldSerializers[i];
        }
        fieldSerializers.clear();
    }

    void* deserialize(DataInputView& source) override;

    void serialize(void* record, DataOutputSerializer& target) override;

    void deserialize(Object *buffer, DataInputView &source) override;

    void serialize(Object *buffer, DataOutputSerializer &target) override;

    const char *getName() const override
    {
        return "TupleSerializer";
    }

    BackendDataType getBackendId() const override;

    void createSerializer(nlohmann::json &type);

    void setSubBufferReusable(bool bufferReusable_) override;

    Object* GetBuffer() override;

private:
    std::vector<TypeSerializer *> fieldSerializers;
    uint16_t arity = 0;
};

#endif
