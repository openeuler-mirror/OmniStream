/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef OMNISTREAM_KEYSELECT_H
#define OMNISTREAM_KEYSELECT_H

#include <memory>
#include "basictypes/Object.h"

template<typename T>
class KeySelect {
public:
    virtual T* getKey(T* value) = 0;
};

template<typename T>
using KeySelectUnique = std::unique_ptr<KeySelect<T>>;
#endif //OMNISTREAM_KEYSELECT_H
