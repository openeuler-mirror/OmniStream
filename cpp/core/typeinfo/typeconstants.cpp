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

const char* TYPE_NAME_VOID = "Void";
const char* TYPE_NAME_LONG = "Long";

const char* TYPE_NAME_INT_SERIALIZER = "org.apache.flink.api.common.typeutils.base.IntSerializer";
const char* TYPE_NAME_BIGINT_SERIALIZER = "org.apache.flink.api.common.typeutils.base.BigIntSerializer";
const char* TYPE_NAME_LONG_SERIALIZER = "org.apache.flink.api.common.typeutils.base.LongSerializer";
const char* TYPE_NAME_DOUBLE_SERIALIZER = "org.apache.flink.api.common.typeutils.base.DoubleSerializer";
const char* TYPE_NAME_TUPLE_SERIALIZER = "org.apache.flink.api.java.typeutils.runtime.TupleSerializer";
const char* TYPE_NAME_POJO_SERIALIZER = "org.apache.flink.api.java.typeutils.runtime.PojoSerializer";
const char* TYPE_NAME_MAP_SERIALIZER = "org.apache.flink.api.common.typeutils.base.MapSerializer";
const char* TYPE_NAME_LIST_SERIALIZER = "org.apache.flink.api.common.typeutils.base.ListSerializer";
const char* TYPE_NAME_VOID_SERIALIZER = "org.apache.flink.api.common.typeutils.base.VoidSerializer";

// basic
const char* TYPE_NAME_STRING = "String";
const char* TYPE_NAME_STRING_CLASS = "java.lang.String";
const char* TYPE_NAME_STRING_CLASS_LINE = "java_lang_String";
const char* TYPE_NAME_STRING_SERIALIZER = "org.apache.flink.api.common.typeutils.base.StringSerializer";

// other
const char* TYPE_NAME_VOID_NAMESPACE = "VoidNamespace";
const char* TYPE_NAME_VOID_NAMESPACE_CLASS = "org.apache.flink.runtime.state.VoidNamespace";
const char* TYPE_NAME_VOID_NAMESPACE_CLASS_LINE = "org_apache_flink_runtime_state_VoidNamespace";
const char* TYPE_NAME_VOID_NAMESPACE_SERIALIZER = "org.apache.flink.runtime.state.VoidNamespaceSerializer";

const char* TYPE_NAME_TIMER = "TimerHeapInternalTimer";
const char* TYPE_NAME_TIMER_CLASS = "org.apache.flink.streaming.api.operators.TimerHeapInternalTimer";
const char* TYPE_NAME_TIMER_CLASS_LINE = "org_apache_flink_streaming_api_operators_TimerHeapInternalTimer";
const char* TYPE_NAME_TIMER_SERIALIZER = "org.apache.flink.streaming.api.operators.TimerSerializer";
