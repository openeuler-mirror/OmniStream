/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/24/25.
//
#include "basictypes/java_lang_ClassLoader.h"
ClassLoader::ClassLoader() = default;
ClassLoader::~ClassLoader() = default;
InputStream* ClassLoader::getResourceAsStream(String * str)
{
    return nullptr;
};
InputStream* ClassLoader::getResourceAsStream(const std::string &str)
{
    return nullptr;
};