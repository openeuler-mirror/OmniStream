/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/26/25.
//

#ifndef THROWABLE_H
#define THROWABLE_H

#include <string>

namespace omnistream {
    class Throwable : std::exception  {
    public:
        explicit Throwable(const std::string& message) : message_(message) {}
        const char* what() const noexcept override { return message_.c_str(); }
        std::string toString() const { return "Throwable(" + message_ + ")"; }
    private:
        std::string message_;
    };
}

#endif  //THROWABLE_H
