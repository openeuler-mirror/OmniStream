/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_PATH_H
#define OMNISTREAM_PATH_H

#include <string>

class Path {
    std::string pathStr;

public:
    Path(const std::string &path) : pathStr(path) {}

    operator std::string() const
    {
        return pathStr;
    }

    const std::string &toString() const
    {
        return pathStr;
    }
};

#endif // OMNISTREAM_PATH_H