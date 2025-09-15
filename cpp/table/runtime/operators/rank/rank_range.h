/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_RANK_RANGE_H
#define FLINK_TNEL_RANK_RANGE_H

// Base class for RankRange.
class RankRange {
public:
    virtual ~RankRange() = default;
};

/// ---------------------------------------------------------------------
/// ConstantRankRange: A RankRange with fixed start and end values.
/// ---------------------------------------------------------------------
class ConstantRankRange : public RankRange {
public:
    ConstantRankRange(long rankStart, long rankEnd);

    // Getters for the constant rank range values.
    long getRankStart() const;
    long getRankEnd() const;

private:
    long rankStart_;
    long rankEnd_;
};

/// ---------------------------------------------------------------------
/// VariableRankRange: A RankRange where the rank end is determined at runtime.
/// ---------------------------------------------------------------------
class VariableRankRange : public RankRange {
public:
    explicit VariableRankRange(int rankEndIndex);

    // Get the index of the rank end field in the input data.
    int getRankEndIndex() const;

private:
    int rankEndIndex_;
};

#endif
