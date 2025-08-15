/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/23/25.
//
#include "basictypes/Class.h"
Class::Class() = default;

Class::Class(const std::string& str) : name(str){ }

Class::Class(std::string &&str) noexcept : name(std::move(str)){ }

std::string_view Class::getName()
{
    return name;
}

ClassLoader* Class::getClassLoader()
{
    return new ClassLoader();
};

Class::~Class() = default;