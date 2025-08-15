/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_WINDOWSTATE_H
#define FLINK_TNEL_WINDOWSTATE_H

template <typename W>
class WindowState {
public:
    virtual void clear(W window) = 0;
    virtual ~WindowState() = default;
};

#endif // FLINK_TNEL_WINDOWSTATE_H
