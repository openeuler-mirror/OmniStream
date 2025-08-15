/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_DUMMYSPLITFETCHERTASK_H
#define OMNISTREAM_DUMMYSPLITFETCHERTASK_H

#include "SplitFetcherTask.h"

class DummySplitFetcherTask : public SplitFetcherTask {
public:
    explicit DummySplitFetcherTask(std::string name) : name(name) {}

    bool Run() override
    {
        return false;
    }

    void WakeUp() override
    {
    }

    std::string ToString() override
    {
        return name;
    }
private:
    std::string name;
};


#endif // OMNISTREAM_DUMMYSPLITFETCHERTASK_H
