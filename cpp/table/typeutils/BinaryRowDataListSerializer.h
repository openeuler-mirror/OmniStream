/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_BINARYROWDATALISTSERIALIZER_H
#define OMNISTREAM_BINARYROWDATALISTSERIALIZER_H

#include "BinaryRowDataSerializer.h"

class BinaryRowDataListSerializer : public TypeSerializerSingleton {
public:
    BinaryRowDataListSerializer() {}

    ~BinaryRowDataListSerializer() {}

    // void * BinaryRowData
    void *deserialize(DataInputView &source) override;

    void serialize(void *row, DataOutputSerializer &target) override;

    [[nodiscard]] const char *getName() const override;

    BackendDataType getBackendId() const override { return BackendDataType::ROW_LIST_BK; };
private:
    BinaryRowDataSerializer* binaryRowDataSerializer = new BinaryRowDataSerializer(1);
};

#endif // OMNISTREAM_BINARYROWDATALISTSERIALIZER_H
