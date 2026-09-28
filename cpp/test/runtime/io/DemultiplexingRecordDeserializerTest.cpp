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

#include <gtest/gtest.h>

#include "runtime/io/network/api/serialization/RecordDeserializer.h"
#include "runtime/io/recover/DemultiplexingRecordDeserializer.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"

namespace {

class DefaultRecordDeserializer : public omnistream::datastream::RecordDeserializer {};

TEST(RecordDeserializerTest, FilterRecordForSqlReturnsOriginalRecordByDefault)
{
    DefaultRecordDeserializer deserializer;
    StreamRecord record;

    EXPECT_EQ(deserializer.FilterRecordForSql(record), &record);
}

TEST(DemultiplexingRecordDeserializerTest, FilterRecordForSqlRequiresSelectedVirtualChannel)
{
    omnistream::DemultiplexingRecordDeserializer deserializer({});
    StreamRecord record;

    EXPECT_THROW(deserializer.FilterRecordForSql(record), std::runtime_error);
}

} // namespace
