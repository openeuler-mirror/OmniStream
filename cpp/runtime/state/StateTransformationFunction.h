/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_STATETRANSFORMATIONFUNCTION_H
#define FLINK_TNEL_STATETRANSFORMATIONFUNCTION_H

template <typename S, typename T>
class StateTransformationFunction
{
public:
    virtual S apply(S previousState, T value) = 0;
};

#endif // FLINK_TNEL_STATETRANSFORMATIONFUNCTION_H
