/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef WINDOW_H
#define WINDOW_H

class Window {
public:
    Window() = default;

    virtual ~Window() = default;

    virtual long maxTimestamp() const = 0;
};

#endif
