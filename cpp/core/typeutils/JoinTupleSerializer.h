/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_JOINTUPLESERIALIZER_H
#define OMNISTREAM_JOINTUPLESERIALIZER_H

#include "TypeSerializerSingleton.h"


class JoinTupleSerializer : public TypeSerializerSingleton {
public:
    JoinTupleSerializer();

    void *deserialize(DataInputView &source) override;
    void serialize(void *record, DataOutputSerializer &target) override;

    static JoinTupleSerializer* instance;

    BackendDataType getBackendId() const override { return BackendDataType::TUPLE_INT32_INT64;};
};


#endif // OMNISTREAM_JOINTUPLESERIALIZER_H
