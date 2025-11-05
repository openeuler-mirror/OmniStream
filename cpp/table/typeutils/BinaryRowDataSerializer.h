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

#ifndef FLINK_TNEL_BINARYROWDATASERIALIZER_H
#define FLINK_TNEL_BINARYROWDATASERIALIZER_H

#include "table/data/binary/BinaryRowData.h"
#include "core/typeutils/TypeSerializerSingleton.h"
#include "table/data/JoinedRowData.h"

class BinaryRowDataSerializer : public TypeSerializerSingleton {
public:
    explicit BinaryRowDataSerializer(int numFields);
    BinaryRowDataSerializer(int numFields, const std::vector<std::string>& inputTypes);
    ~BinaryRowDataSerializer() override;

    // void * BinaryRowData
    void *deserialize(DataInputView &source) override;

    void serialize(void *row, DataOutputSerializer &target) override;

    void serialize(Object *row, DataOutputSerializer &target) override;

    [[nodiscard]] const char *getName() const override;

    static BinaryRowData* joinedRowToBinaryRow(JoinedRowData *row, const std::vector<int32_t>& typeId = {});

    static BinaryRowData* joinedRowFromBothBinaryRow(JoinedRowData *row);

    BackendDataType getBackendId() const override { return BackendDataType::ROW_BK;};

    const std::vector<std::string>& getInputTypes() const;
private:
    // Add JoinedRowDataSerializer, then pass the unconverted JoinedRowData to
    // output collector instead of the converted BinaryRowData
    int numFields_;
    int fixedLengthPartSize_;
    BinaryRowData* reUse_;
    std::vector<std::string> inputTypes_;

    const static int SEG_SIZE = 2048;
};

#endif
