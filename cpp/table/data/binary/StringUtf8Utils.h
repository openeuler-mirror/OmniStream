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

#ifndef FLINK_TNEL_STRINGUTF8UTILS_H
#define FLINK_TNEL_STRINGUTF8UTILS_H

#include <string>
#include <vector>
#include <cstdint> // For uint8_t
#include <stdexcept>


class StringUtf8Utils {
    static const int MAX_BYTES_PER_CHAR = 3;

public:
    // Decoder and Encoder
    static std::u32string* decodeUTF8(uint8_t *bytes, int offset, int len);
    static uint8_t *encodeUTF8(const std::u32string *str);

    static int computeUTF8Length(const std::u32string *str);
};

#endif // FLINK_TNEL_STRINGUTF8UTILS_H
