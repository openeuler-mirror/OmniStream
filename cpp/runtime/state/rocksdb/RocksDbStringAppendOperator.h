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
#pragma once
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

class RocksDbStringAppendOperator : public rocksdb::AssociativeMergeOperator {
public:
    explicit RocksDbStringAppendOperator(char delimiter) : delimiter(delimiter) {}

    bool Merge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value, const rocksdb::Slice& value,
               std::string* new_value, rocksdb::Logger* logger) const override
    {
        new_value->clear();
        if (!existing_value) {
            new_value->assign(value.data(), value.size());
        } else {
            new_value->reserve(existing_value->size() + 1 + value.size());
            new_value->assign(existing_value->data(), existing_value->size());
            new_value->append(1, delimiter);
            new_value->append(value.data(), value.size());
        }
        return true;
    };

    [[nodiscard]] const char* Name() const override
    {
        return "RocksDbStringAppendOperator";
    }

private:
    char delimiter;         // The delimiter is inserted between elements
};
