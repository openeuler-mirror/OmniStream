/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by c00572813 on 2025/2/13.
//

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
