/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_DATAINPUTVIEW_H
#define FLINK_TNEL_DATAINPUTVIEW_H

#include "../utils/SysDataInput.h"

class DataInputView : public SysDataInput {
public:
    virtual void *GetBuffer() = 0;
    ~DataInputView() override = default;
};
#endif  //FLINK_TNEL_DATAINPUTVIEW_H
