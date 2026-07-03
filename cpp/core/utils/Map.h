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

#pragma once

namespace omnistream::utils {
template <typename K, typename V>
class Map {
public:
    virtual ~Map() = default;

    class Entry {
    public:
        virtual ~Entry() = default;
        virtual std::optional<K> getKey() = 0;
        virtual std::optional<V> getValue() = 0;
        virtual void setValue(std::optional<V> value) = 0;
    };
};
} // namespace omnistream::utils
