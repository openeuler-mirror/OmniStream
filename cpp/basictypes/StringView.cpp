/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/StringView.h"

StringView::StringView() = default;

StringView::StringView(char *pointer, size_t length)
{
    this->data = pointer;
    this->size = length;
}

StringView::~StringView() = default;

void StringView::setData(char *pointer)
{
    this->data = pointer;
}

void StringView::setSize(size_t length)
{
    this->size = length;
}

char *StringView::getData()
{
    return data;
}

size_t StringView::getSize()
{
    return size;
}

std::string_view StringView::getValue()
{
    return {data, size};
}

std::string StringView::toString()
{
    return std::string(data, size);
}
