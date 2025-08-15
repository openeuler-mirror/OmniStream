/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/26/25.
//

#include "ObjectBufferRecycler.h"

#include <memory>
namespace omnistream
{
    // Define the static member
    std::shared_ptr<DummyObjectBufferRecycler> DummyObjectBufferRecycler::instance = nullptr;
}
