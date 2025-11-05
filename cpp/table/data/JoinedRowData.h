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
#ifndef FLINK_TNEL_JOINEDROWDATA_H
#define FLINK_TNEL_JOINEDROWDATA_H

#include "table/data/RowData.h"
#include "table/data/TimestampData.h"
#include "table/data/RowKind.h"

class JoinedRowData : public RowData {
public:
    JoinedRowData();
    JoinedRowData(RowData* row1_, RowData* row2_);
    JoinedRowData(RowKind rowKind_, RowData* row1_, RowData* row2_);

    ~JoinedRowData() override = default;

    JoinedRowData* replace(RowData* row1_, RowData* row2_);

    int getArity() override;
    RowKind getRowKind() override;
    void setRowKind(RowKind kind) override;
    bool isNullAt(int pos) override;
    long *getLong(int pos) override;
    bool *getBool(int pos) override;
    int* getInt(int pos) override;
    TimestampData *getTimestamp(int pos) override;
    TimestampData *getTimestampPrecise(int pos) override;
    std::string_view getStringView(int pos);
    void setStringView(int pos, std::string_view value);

    RowData* getRow1();
    RowData* getRow2();

    void setLong(int pos, long value) override;

    void setInt(int pos, int value) override;

    void setTimestamp(int pos, TimestampData& value, int precision) override;

    void setString(int pos, BinaryStringData* value) override;

    int hashCode() const override;
    [[nodiscard]] int hashCodeFast() const override;
    bool operator==(const RowData &other) const override;

private:
    RowKind rowKind;
    RowData* row1;
    RowData* row2;
};

#endif // FLINK_TNEL_JOINEDROWDATA_H
