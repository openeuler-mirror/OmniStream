/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/23/25.
//
#include "thirdlibrary/org_slf4j_Logger.h"
#include <iostream>

Logger::Logger() = default;
Logger::~Logger() = default;
void Logger::info(String *info)
{
    std::cout << "this is custom logger" << std::endl;

    std::cout << info->toString() << std::endl;
}

void Logger::info(std::string format, String * info)
{
    std::cout << "this is custom logger" << std::endl;
    std::cout << format << info->toString() << std::endl;
}

void Logger::info(String * format, String * info)
{
    std::cout << "this is custom logger" << std::endl;
    std::cout << format->toString() << info->toString() << std::endl;
}

void Logger::warn(String *warn)
{
    std::cout << "this is custom logger" << std::endl;
    std::cout << warn->toString() << std::endl;
}

void Logger::error(String *error)
{
    std::cout << "this is custom logger" << std::endl;
    std::cout << error->toString() << std::endl;
}

void Logger::warn(String *warn, Throwable *e)
{
    std::cout << "this is custom logger" << std::endl;
    std::cout << warn->toString() << std::endl;
}

void Logger::warn(std::string str, Object * obj1, Object * obj2)
{
    std::cout << "this is custom logger" << std::endl;
    std::cout << str << std::endl;
}

void Logger::error(String *error, Throwable *e)
{
    std::cout << "this is custom logger" << std::endl;
    std::cout << error->toString() << std::endl;
}

void Logger::error(std::string str)
{
    std::cout << str << std::endl;
}
