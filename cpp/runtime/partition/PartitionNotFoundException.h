/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef PARTITION_NOT_FOUND_EXCEPTION_H
#define PARTITION_NOT_FOUND_EXCEPTION_H

#include <stdexcept>
#include <string>

class PartitionNotFoundException : public std::logic_error {
public:
    explicit PartitionNotFoundException(const std::string& message)
        : std::logic_error(message) {}
};

#endif // PARTITION_NOT_FOUND_EXCEPTION_H