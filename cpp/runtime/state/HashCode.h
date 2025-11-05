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


#ifndef OMNISTREAM_HASHCODE_H
#define OMNISTREAM_HASHCODE_H

#include <cstdint>

class HashCode {
public:
    static uint32_t Hash(const uint8_t *data, uint32_t dataLen)
    {
        uint32_t hash = 1;
        for (uint32_t i = 0; i < dataLen; i++) {
            hash = 31 * hash + data[i];
        }
        return hash;
    }
};

#endif // OMNISTREAM_HASHCODE_H
