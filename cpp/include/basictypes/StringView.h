/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/5/25.
//

#ifndef FLINK_TNEL_STRINGVIEW_H
#define FLINK_TNEL_STRINGVIEW_H

#include "String.h"

/**
 * data come from other, which is used in kafka source operator
 */
class StringView : public String {
public:
    StringView();

    StringView(char *pointer, size_t length);

    ~StringView();

    void setData(char *pointer) override;

    void setSize(size_t length) override;

    char* getData() override;

    size_t getSize() override;

    std::string_view getValue() override;

    std::string toString() override;

private:
    char *data;
    size_t size;
};

#endif //FLINK_TNEL_STRINGVIEW_H
