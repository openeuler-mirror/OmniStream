/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_SPLITFETCHERTASK_H
#define OMNISTREAM_SPLITFETCHERTASK_H

#include <string>

class SplitFetcherTask {
public:
    virtual bool Run() = 0;
    virtual void WakeUp() = 0;
    virtual std::string ToString() = 0;
    virtual ~SplitFetcherTask() = default;
};

#endif // OMNISTREAM_SPLITFETCHERTASK_H
