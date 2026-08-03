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

#ifndef OMNISTREAM_BSSKEYGROUPUTILS_H
#define OMNISTREAM_BSSKEYGROUPUTILS_H
#ifdef WITH_OMNISTATESTORE

#include <cstdint>

/**
 * BSS 的 keyHashCode 契约：AbstractTable::GetStateId 以 keyHashCode % maxParallelism
 * 推导 key group，要求其等于该 key 的 Flink key group（Java 插件传 Flink murmur hash
 * 天然满足）。OmniStream 侧的原始字节 hash 与 Flink 分组无关，直接传会导致大量
 * "wrong keyGroupIndex" + stateId=0，checkpoint 分组元数据错误，恢复/rescale 丢数据。
 * 本工具把原始 hash 的低位调整为指定 key group，同时保留其余位的熵用于桶分布。
 */
class BssKeyGroupUtils {
public:
    static uint32_t ForceKeyGroup(uint32_t rawHash, uint32_t keyGroup, uint32_t maxParallelism)
    {
        if (maxParallelism == 0) {
            return rawHash;
        }
        uint64_t adjusted = static_cast<uint64_t>(rawHash) - (rawHash % maxParallelism) + (keyGroup % maxParallelism);
        if (adjusted > UINT32_MAX) {
            adjusted -= maxParallelism;
        }
        return static_cast<uint32_t>(adjusted);
    }
};

#endif // WITH_OMNISTATESTORE
#endif // OMNISTREAM_BSSKEYGROUPUTILS_H
