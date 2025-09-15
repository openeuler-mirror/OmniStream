/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_JOINTUPLESERIALIZER2_H
#define OMNISTREAM_JOINTUPLESERIALIZER2_H

#include "TypeSerializerSingleton.h"


class JoinTupleSerializer2 : public TypeSerializerSingleton {
public:
    JoinTupleSerializer2();

    void *deserialize(DataInputView &source) override;
    void serialize(void *record, DataOutputSerializer &target) override;

    static JoinTupleSerializer2* instance;

    BackendDataType getBackendId() const override { return BackendDataType::TUPLE_INT32_INT32_INT64;};
};


#endif // OMNISTREAM_JOINTUPLESERIALIZER_H
