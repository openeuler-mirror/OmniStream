/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef CSVCONVERTER_H
#define CSVCONVERTER_H

#pragma once

#include "CsvRow.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/types/logical/LogicalType.h"
#include "type/data_type.h"
#include "table/data/binary/BinaryStringData.h"
#include "table/data/TimestampData.h"
#include "table/vectorbatch/VectorBatch.h"
#include "vector/vector_helper.h"

using namespace omniruntime::type;

namespace omnistream {
namespace csv {

class CsvConverter {
public:
    static BinaryRowData* convert(const CsvRow& csvRow);

    /**
     * @brief Convert csv rows to vector batch using the first row's schema as the target shcema
     * @param csvRows
     * @return VectorBatch
     */
    static omnistream::VectorBatch *convert(std::vector<CsvRow> &csvRows);

    /**
     * @brief Convert csv rows to vector batch using the given `oneMap`
     * @param csvRows
     * @param oneMap mapping from project field index to csv field index
     * @return VectorBatch
     */
    static omnistream::VectorBatch *convert(std::vector<CsvRow> &csvRows, std::vector<int>& oneMap);
};

}  // namespace csv
}  // namespace omnistream

#endif
