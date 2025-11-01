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
#ifndef OMNISTREAM_BUFFER_H
#define OMNISTREAM_BUFFER_H

#include <cstdint>
#include <vector>
#include <cstddef>

class Buffer {
public:
    explicit Buffer(std::size_t size) : data(size) {}

    ~Buffer() = default;

    const uint8_t* Data() const
    {
        return data.data();
    }
    
    uint8_t* Data()
    {
        return data.data();
    }
    
    std::size_t Size() const
    {
        return data.size();
    }

    void Recycle() {}

private:
    std::vector<uint8_t> data;
};

#endif // OMNISTREAM_BUFFER_H
