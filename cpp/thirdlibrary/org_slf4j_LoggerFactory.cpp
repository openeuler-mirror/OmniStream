/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "thirdlibrary/org_slf4j_LoggerFactory.h"
LoggerFactory::LoggerFactory() = default;
LoggerFactory::~LoggerFactory() = default;

Logger* LoggerFactory::getLogger(Class * cls)
{
    return new Logger();
}