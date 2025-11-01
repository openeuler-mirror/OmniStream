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
#ifndef WINDOW_H
#define WINDOW_H

class Window {
public:
    Window() = default;

    virtual ~Window() = default;

    virtual long maxTimestamp() const = 0;

protected:
    // Copy constructor and assignment operator are protected to prevent object slicing
    Window(const Window&) = default;
    Window& operator=(const Window&) = default;
    Window(Window&&) = default;
    Window& operator=(Window&&) = default;
};

#endif
