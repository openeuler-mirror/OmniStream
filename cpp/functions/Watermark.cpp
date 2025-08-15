/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/21/24.
//

#include "functions/Watermark.h"

const Watermark Watermark::MAX_WATERMARK (LONG_MAX);

Watermark::Watermark(int64_t timestamp) : timestamp_(timestamp)
{
    setTag(StreamElementTag::TAG_WATERMARK);
}

int64_t Watermark::getTimestamp() const
{
    return timestamp_;
}

void Watermark::setTimestamp(int64_t timestamp)
{
    timestamp_ = timestamp;
}
