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
#ifndef OMNISTREAM_COMPOSITEKEYSERIALIZATIONUTILS_H
#define OMNISTREAM_COMPOSITEKEYSERIALIZATIONUTILS_H

#include <cstdint>
#include <vector>

class CompositeKeySerializationUtils {
public:
    static constexpr int BITS_PER_BYTE = 8;
    static constexpr int SINGLE_BYTE_MAX_GROUPS = 128;
    static constexpr int PREFIX_TWO_BYTES = 2;
    static constexpr int PREFIX_ONE_BYTES = 1;

    static void serializeKeyGroup(int keyGroup, std::vector<uint8_t>& startKeyGroupPrefixBytes)
    {
        const size_t keyGroupPrefixBytes = startKeyGroupPrefixBytes.size();
        for (size_t j = 0; j < keyGroupPrefixBytes; ++j) {
            startKeyGroupPrefixBytes[j] = extractByteAtPosition(keyGroup, keyGroupPrefixBytes - j - 1);
        }
    }

    static uint8_t extractByteAtPosition(int value, int byteIdx)
    {
        return static_cast<uint8_t>((value >> (byteIdx * BITS_PER_BYTE)));
    }

    static int computeRequiredBytesInKeyGroupPrefix(int totalKeyGroupsInJob)
    {
        return totalKeyGroupsInJob > SINGLE_BYTE_MAX_GROUPS ? PREFIX_TWO_BYTES : PREFIX_ONE_BYTES;
    }
};

#endif // OMNISTREAM_COMPOSITEKEYSERIALIZATIONUTILS_H
