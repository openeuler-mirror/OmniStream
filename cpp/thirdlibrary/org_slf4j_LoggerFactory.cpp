/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/23/25.
//
#include "thirdlibrary/org_slf4j_LoggerFactory.h"
LoggerFactory::LoggerFactory() = default;
LoggerFactory::~LoggerFactory() = default;

Logger* LoggerFactory::getLogger(Class * cls)
{
    return new Logger();
}