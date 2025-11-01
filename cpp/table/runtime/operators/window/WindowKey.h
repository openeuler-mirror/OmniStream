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

#ifndef FLINK_TNEL_WINDOWKEY_H
#define FLINK_TNEL_WINDOWKEY_H


#include <iostream>
#include <functional>
#include "table/data/binary/BinaryRowData.h"

class WindowKey {
public:
    WindowKey(long window, RowData* key) : window(window), key(key) {}
    WindowKey replace(long window, RowData* key);
    long getWindow() const;
    RowData* getKey() const;
    long hash() const;
    // 重载相等比较运算符
    bool operator==(const WindowKey& other) const
    {
        return window == other.window && *key == *(other.getKey());
    }
private:
    long window;
    RowData* key;
};

namespace std {
    template<>
    struct hash<WindowKey> {
        std::size_t operator()(const WindowKey& windowKey) const noexcept
        {
            return windowKey.hash();
        }
    };
}


#endif
