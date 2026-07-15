/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2. You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PSL v2 for more details.
 */

#include "runtime/state/VectorBatchStateAccessor.h"

#include "table/data/util/VectorBatchUtil.h"

std::unique_ptr<RowData> VectorBatchStateAccessor::getRow(int64_t comboId)
{
    return getRow(VectorBatchUtil::getBatchId(comboId), VectorBatchUtil::getRowId(comboId));
}
