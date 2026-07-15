/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <memory>
#include <vector>

#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"

namespace omnistream {

class RestoreKVState;
class RestoreKVStateVB;

// RestoreBackendDelegate 是 compatible restore 期间 construction backend 的 writer factory。adaptor 只能通过
// 它创建固定 kvStateId/keyGroupId 的普通或 VectorBatch KV writer，不能直接访问 Heap StateTable、RocksDB
// column family 或 write batch。writer 的具体协议与实现由后续 adaptor/backend 写入模块提供。
class RestoreBackendDelegate {
public:
    virtual ~RestoreBackendDelegate() = default;

    virtual std::unique_ptr<RestoreKVState> createKVState(
        int kvStateId, int keyGroupId, const StateMetaInfoSnapshot& mainMetaInfo) = 0;

    virtual std::unique_ptr<RestoreKVStateVB> createKVStateVB(
        int kvStateId,
        int keyGroupId,
        const StateMetaInfoSnapshot& mainMetaInfo,
        const std::vector<omniruntime::type::DataTypeId>& columnTypes,
        int vectorBatchSize) = 0;
};

} // namespace omnistream
