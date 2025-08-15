/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef OMNISTREAM_virtual_enable_shared_from_this_base_H
#define OMNISTREAM_virtual_enable_shared_from_this_base_H

#include <memory>

namespace omnistream {
struct virtual_enable_shared_from_this_base : std::enable_shared_from_this<virtual_enable_shared_from_this_base> {
public:
    virtual ~virtual_enable_shared_from_this_base()
    {}
};
template <typename T>
struct virtual_enable_shared_from_this : virtual virtual_enable_shared_from_this_base {
public:
    std::shared_ptr<T> shared_from_this()
    {
        return std::dynamic_pointer_cast<T>(virtual_enable_shared_from_this_base::shared_from_this());
    }
};
}  // namespace omnistream

#endif  // OMNISTREAM_virtual_enable_shared_from_this_base_H