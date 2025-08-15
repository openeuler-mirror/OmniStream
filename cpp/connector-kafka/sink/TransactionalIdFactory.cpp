/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */
#include <random>
#include <string>
#include "ctime"
#include "TransactionalIdFactory.h"

const std::string TransactionalIdFactory::TRANSACTIONAL_ID_DELIMITER = "-";

std::string generateRandomString(size_t length)
{
    const std::string chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<size_t> distribution(0, chars.size() - 1);

    std::string randomString;
    for (size_t i = 0; i < length; ++i) {
        randomString += chars[distribution(generator)];
    }

    return randomString;
}

std::string TransactionalIdFactory::buildTransactionalId(const std::string& transactionalIdPrefix,
                                                         int subtaskId,
                                                         long checkpointOffset)
{
    return transactionalIdPrefix
           + TRANSACTIONAL_ID_DELIMITER
           + std::to_string(subtaskId)
           + TRANSACTIONAL_ID_DELIMITER
           + std::to_string(checkpointOffset)
           + generateRandomString(randomStrLen);
}