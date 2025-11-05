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
#ifndef CSVCONVERTER_H
#define CSVCONVERTER_H

#pragma once

#include "CsvRow.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/types/logical/LogicalType.h"
#include "type/data_type.h"
#include "table/data/binary/BinaryStringData.h"
#include "table/data/TimestampData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "vector/vector_helper.h"

using namespace omniruntime::type;

namespace omnistream {
namespace csv {

class CsvConverter {
public:
    static BinaryRowData* convert(const CsvRow& csvRow);

    /**
     * @brief Convert csv rows to vector batch using the first row's schema as the target schema
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

} // namespace csv
} // namespace omnistream

#endif
