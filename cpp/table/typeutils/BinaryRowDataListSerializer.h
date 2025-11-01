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
