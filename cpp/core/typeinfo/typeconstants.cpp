/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * @Description: Tuple Type Info for DataStream
 */

#include "typeconstants.h"

const char* TYPE_NAME_STRING = "String";
const char* TYPE_NAME_VOID = "Void";
const char* TYPE_NAME_LONG = "Long";

const char* TYPE_NAME_STRING_SERIALIZER = "org.apache.flink.api.common.typeutils.base.StringSerializer";
const char* TYPE_NAME_INT_SERIALIZER = "org.apache.flink.api.common.typeutils.base.IntSerializer";
const char* TYPE_NAME_BIGINT_SERIALIZER = "org.apache.flink.api.common.typeutils.base.BigIntSerializer";
const char* TYPE_NAME_LONG_SERIALIZER = "org.apache.flink.api.common.typeutils.base.LongSerializer";
const char* TYPE_NAME_DOUBLE_SERIALIZER = "org.apache.flink.api.common.typeutils.base.DoubleSerializer";
const char* TYPE_NAME_TUPLE_SERIALIZER = "org.apache.flink.api.java.typeutils.runtime.TupleSerializer";
const char* TYPE_NAME_POJO_SERIALIZER = "org.apache.flink.api.java.typeutils.runtime.PojoSerializer";
const char* TYPE_NAME_MAP_SERIALIZER = "org.apache.flink.api.common.typeutils.base.MapSerializer";
const char* TYPE_NAME_LIST_SERIALIZER = "org.apache.flink.api.common.typeutils.base.ListSerializer";
