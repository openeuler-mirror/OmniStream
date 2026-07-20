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

template <typename K>
class HeapKeyedStateBackend;

// HeapRestoreBackendDelegate 为 compatible restore 的 construction Heap backend 创建目标 state writer。
// 当前只固定 interface 和 construction backend 所有权边界；writer 实现尚未落地时，
// 各 factory 均 fail-fast，调用方不得把该临时实现当作可完成真实恢复的 writer。
template <typename K>
class HeapRestoreBackendDelegate final : public omnistream::RestoreBackendDelegate {
public:
    explicit HeapRestoreBackendDelegate(HeapKeyedStateBackend<K>* constructionBackend)
        : constructionBackend_(constructionBackend)
    {
    }

    std::unique_ptr<omnistream::RestoreKVState> createKVState(int, const StateMetaInfoSnapshot&) override
    {
        throw std::logic_error("Heap compatible restore KV writer is not implemented");
    }

    std::unique_ptr<omnistream::RestoreKVStateVB> createKVStateVB(
        int, const StateMetaInfoSnapshot&, const std::vector<omniruntime::type::DataTypeId>&, int) override
    {
        throw std::logic_error("Heap compatible restore VectorBatch KV writer is not implemented");
    }

    std::unique_ptr<omnistream::RestorePQState> createPQState(int, const StateMetaInfoSnapshot&) override
    {
        throw std::logic_error("Heap compatible restore PriorityQueue writer is not implemented");
    }

private:
    HeapKeyedStateBackend<K>* constructionBackend_;
};
