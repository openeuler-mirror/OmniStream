/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/15/24.
//

#include "ByteBufferView.h"

ByteBufferView::~ByteBufferView() {
    // do not delete data_
}

ByteBufferView::ByteBufferView() = default;

ByteBufferView *ByteBufferView::wrap(std::vector<uint8_t>* data) {
    auto ret = new ByteBufferView();
    ret->capacity_ = data->capacity();
    ret->clear();
    return ret;
}
