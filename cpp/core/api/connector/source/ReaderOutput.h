/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_READEROUTPUT_H
#define FLINK_TNEL_READEROUTPUT_H

#include <memory>
#include <string>
#include "SourceOutput.h"

class ReaderOutput : public SourceOutput {
public:
    virtual SourceOutput& CreateOutputForSplit(const std::string& splitId) = 0;

    virtual void ReleaseOutputForSplit(const std::string& splitId) = 0;
};


#endif // FLINK_TNEL_READEROUTPUT_H
