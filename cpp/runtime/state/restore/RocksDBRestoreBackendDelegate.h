/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "runtime/state/restore/RestoreBackendDelegate.h"

class RocksDbHandle;

// RocksDBRestoreBackendDelegate 为 compatible restore 的 construction RocksDB handle 创建目标 state writer。
// 当前只固定 column family/write batch writer 的接口和 handle 生命周期边界；真实 writer 未落地时，factory
// 直接 fail-fast，不能返回空 writer 或将 Flink logical bytes 写入现有 raw direct put 路径。
class RocksDBRestoreBackendDelegate final : public omnistream::RestoreBackendDelegate {
public:
    explicit RocksDBRestoreBackendDelegate(RocksDbHandle* constructionHandle) : constructionHandle_(constructionHandle)
    {
    }

    std::unique_ptr<omnistream::RestoreKVState> createKVState(int, const StateMetaInfoSnapshot&) override
    {
        throw std::logic_error("RocksDB compatible restore KV writer is not implemented");
    }

    std::unique_ptr<omnistream::RestoreKVStateVB> createKVStateVB(
        int, const StateMetaInfoSnapshot&, const std::vector<omniruntime::type::DataTypeId>&, int) override
    {
        throw std::logic_error("RocksDB compatible restore VectorBatch KV writer is not implemented");
    }

    std::unique_ptr<omnistream::RestorePQState> createPQState(int, const StateMetaInfoSnapshot&) override
    {
        throw std::logic_error("RocksDB compatible restore PriorityQueue writer is not implemented");
    }

private:
    RocksDbHandle* constructionHandle_;
};
