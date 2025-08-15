/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by arpit on 1/31/25.
//

#include "rank_range.h"

// --- ConstantRankRange Implementation ---

ConstantRankRange::ConstantRankRange(long rankStart, long rankEnd)
    : rankStart_(rankStart), rankEnd_(rankEnd) {}

long ConstantRankRange::getRankStart() const
{
    return rankStart_;
}

long ConstantRankRange::getRankEnd() const
{
    return rankEnd_;
}

// --- VariableRankRange Implementation ---

VariableRankRange::VariableRankRange(int rankEndIndex)
    : rankEndIndex_(rankEndIndex) {}

int VariableRankRange::getRankEndIndex() const
{
    return rankEndIndex_;
}
