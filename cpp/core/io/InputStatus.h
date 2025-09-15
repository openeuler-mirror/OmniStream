/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_INPUTSTATUS_H
#define FLINK_TNEL_INPUTSTATUS_H


// 定义 InputStatus 枚举类
enum class InputStatus {
    // 指示有更多数据可用，并且可以立即再次调用输入以产生更多数据
    MORE_AVAILABLE,
    // 指示当前没有数据可用，但未来还会有更多数据可用
    NOTHING_AVAILABLE,
    // 指示输入已经到达数据的末尾
    END_OF_INPUT
};


#endif  //FLINK_TNEL_INPUTSTATUS_H
