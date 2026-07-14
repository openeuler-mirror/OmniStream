/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

/**
 * HeapSnapshotStateData 承载 Heap compatible savepoint 转换阶段需要复用的
 * VectorBatch 侧表 frozen bytes。
 *
 * 普通 Heap snapshot 只使用 HeapSingleStateIterator 内部的 vector entries，不应为了
 * 非 compatible 流程构造本对象。只有显式启用 VB accessor 数据捕获时，调用方才通过
 * addVectorBatchEntry() 追加 VectorBatch 侧表记录并建立 batchId 到最新记录的索引。
 * 若调用方重排 entries，
 * 需要保持相同 batchId 的相对写入顺序并调用 rebuildVectorBatchEntryIndices()，
 * 这样重复 batchId 才能继续指向最后写入的记录。该类只拥有拷贝后的
 * key/value 字节和索引，不持有 live Heap state、serializer 或 VectorBatch 指针，
 * 因此可被 shared_ptr 持有并跨越局部构造者生命周期读取。
 *
 * entries() 与 findVectorBatchEntry() 返回的引用/指针均指向类内部存储；调用方
 * 不应在继续追加记录后长期缓存旧指针，因为 vector 扩容可能使旧指针失效。
 * 查询不存在的 batchId 返回 nullptr。普通 addEntry() 不写入 batchId 索引，并会
 * 强制把 vectorBatchId 归一为 -1，避免普通状态记录被误解释为 VectorBatch entry。
 *
 * Heap snapshot frozen data 与 iterator close 解耦：iterator 关闭只结束迭代生命周期，
 * 不应清理本对象中的 frozen bytes，资源层可继续通过 shared_ptr 创建 VB accessor。
 */
class HeapSnapshotStateData {
public:
    struct SerializedEntry {
        std::vector<int8_t> serializedKey;
        std::vector<int8_t> serializedValue;
        int64_t vectorBatchId = -1;
    };

    // 记录注册状态名，便于资源层按 logical/vb state name 建立访问关系。
    std::string stateName;

    void addEntry(SerializedEntry entry)
    {
        entry.vectorBatchId = -1;
        entries_.push_back(std::move(entry));
    }

    void addVectorBatchEntry(SerializedEntry entry, int64_t batchId)
    {
        entry.vectorBatchId = batchId;
        entries_.push_back(std::move(entry));
        vectorBatchEntryIndices_[batchId] = entries_.size() - 1;
    }

    const SerializedEntry* findVectorBatchEntry(int64_t batchId) const
    {
        auto iter = vectorBatchEntryIndices_.find(batchId);
        if (iter == vectorBatchEntryIndices_.end()) {
            return nullptr;
        }
        return &entries_[iter->second];
    }

    const std::vector<SerializedEntry>& entries() const
    {
        return entries_;
    }

    std::vector<SerializedEntry>& entries()
    {
        return entries_;
    }

    void rebuildVectorBatchEntryIndices()
    {
        vectorBatchEntryIndices_.clear();
        for (std::size_t i = 0; i < entries_.size(); ++i) {
            if (entries_[i].vectorBatchId >= 0) {
                vectorBatchEntryIndices_[entries_[i].vectorBatchId] = i;
            }
        }
    }

private:
    std::vector<SerializedEntry> entries_;
    std::unordered_map<int64_t, std::size_t> vectorBatchEntryIndices_;
};
