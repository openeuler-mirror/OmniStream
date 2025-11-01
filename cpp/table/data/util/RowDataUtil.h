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
#ifndef FLINK_TNEL_ROW_DATA_UTIL_H
#define FLINK_TNEL_ROW_DATA_UTIL_H

#include "../RowData.h"
#include "../RowKind.h"

class RowDataUtil {
public:
    static bool isAccumulateMsg(RowKind row);

    static bool isRetractMsg(RowKind row);
};

#endif // FLINK_TNEL_ROW_DATA_UTIL_H
