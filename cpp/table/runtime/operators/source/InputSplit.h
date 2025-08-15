/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once

#include <string>

namespace omnistream {

class InputSplit {
public:
    InputSplit(std::string filePath, int startOffset, int length)
        : filePath_(filePath), startOffset_(startOffset), length_(length) {}

    std::string getFilePath()
    {
        return filePath_;
    }

    int getStartOffset()
    {
        return startOffset_;
    }

    int getLength()
    {
        return length_;
    }

private:
    std::string filePath_;
    int startOffset_;
    int length_;
};

} // namespace omnistream