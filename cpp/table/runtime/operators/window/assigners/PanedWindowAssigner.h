/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <cstdint>
#include <type_traits>
#include <vector>

#include "WindowAssigner.h"
#include "table/runtime/operators/window/Window.h"

template<typename W>
class PanedWindowAssigner : public WindowAssigner<W> {
    static_assert(std::is_base_of_v<Window, W>, "typename W must inherit from Window");

public:
    ~PanedWindowAssigner() override = default;

    virtual W assignPane(const RowData* element, int64_t timestamp) = 0;

    virtual std::vector<W> splitIntoPanes(const W& window) = 0;

    virtual W getLastWindow(const W& pane) = 0;
};
