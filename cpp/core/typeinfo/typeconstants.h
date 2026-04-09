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

#ifndef FLINK_TNEL_TYPECONSTANTS_H
#define FLINK_TNEL_TYPECONSTANTS_H

extern const char* TYPE_NAME_VOID;
extern const char* TYPE_NAME_LONG;
extern const char* TYPE_NAME_LIST;

extern const char* TYPE_NAME_INT_SERIALIZER;
extern const char* TYPE_NAME_BIGINT_SERIALIZER;
extern const char* TYPE_NAME_LONG_SERIALIZER;
extern const char* TYPE_NAME_DOUBLE_SERIALIZER;
extern const char* TYPE_NAME_TUPLE_SERIALIZER;
extern const char* TYPE_NAME_POJO_SERIALIZER;
extern const char* TYPE_NAME_MAP_SERIALIZER;
extern const char* TYPE_NAME_LIST_SERIALIZER;
extern const char* TYPE_NAME_VOID_SERIALIZER;

// basic
extern const char* TYPE_NAME_STRING;
extern const char* TYPE_NAME_STRING_CLASS;
extern const char* TYPE_NAME_STRING_CLASS_LINE;
extern const char* TYPE_NAME_STRING_SERIALIZER;

// other
extern const char* TYPE_NAME_VOID_NAMESPACE;
extern const char* TYPE_NAME_VOID_NAMESPACE_CLASS;
extern const char* TYPE_NAME_VOID_NAMESPACE_CLASS_LINE;
extern const char* TYPE_NAME_VOID_NAMESPACE_SERIALIZER;

extern const char* TYPE_NAME_TIMER;
extern const char* TYPE_NAME_TIMER_CLASS;
extern const char* TYPE_NAME_TIMER_CLASS_LINE;
extern const char* TYPE_NAME_TIMER_SERIALIZER;

#endif  // FLINK_TNEL_TYPECONSTANTS_H
