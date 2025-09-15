/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_XXH128_HASHSERIALIZER_H
#define OMNISTREAM_XXH128_HASHSERIALIZER_H

#include <xxhash.h>
#include "TypeSerializerSingleton.h"

class XxH128_hashSerializer : public TypeSerializerSingleton {
public:
    XxH128_hashSerializer();

    void *deserialize(DataInputView &source) override;
    void serialize(void *record, DataOutputSerializer &target) override;

    static XxH128_hashSerializer* instance;

    BackendDataType getBackendId() const override { return BackendDataType::XXHASH128_BK;};
};


#endif // OMNISTREAM_XXH128_HASHSERIALIZER_H
