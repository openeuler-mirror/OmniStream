/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <memory>
#include "WatermarkGeneratorSupplier.h"
#include "TimestampAssignerSupplier.h"
#include "WatermarkAlignmentParams.h"
#include "NoWatermarksGenerator.h"
#include "AscendingTimestampsWatermarks.h"


class WatermarkStrategy : public TimestampAssignerSupplier, public WatermarkGeneratorSupplier {
public:

    TimestampAssigner* CreateTimestampAssigner() override {
        return new RecordTimestampAssigner();
    }

    virtual const WatermarkAlignmentParams* GetAlignmentParameters() {
        return WatermarkAlignmentParams::watermarkAlignmentDisabled;
    }

    static std::shared_ptr<WatermarkStrategy> ForMonotonousTimestamps();

    static std::shared_ptr<WatermarkStrategy> ForBoundedOutOfOrderness(int32_t rowtimeFieldIndex, long maxOutOfOrderness);

    static std::shared_ptr<WatermarkStrategy> NoWatermarks();
};

class NoWatermarkStrategy : public WatermarkStrategy {
public:
    WatermarkGenerator* CreateWatermarkGenerator() override {
        return new NoWatermarksGenerator();
    }
};

class BoundedOutOfOrdernessStrategy : public WatermarkStrategy {
public:
    BoundedOutOfOrdernessStrategy(int32_t rowtimeFieldIndex, long maxOutOfOrderness)
            : rowtimeFieldIndex_(rowtimeFieldIndex), maxOutOfOrderness_(maxOutOfOrderness) {}

    WatermarkGenerator* CreateWatermarkGenerator() override {
        return new BoundedOutOfOrdernessWatermarks(maxOutOfOrderness_);
    }

    TimestampAssigner* CreateTimestampAssigner() override {
        return new RecordTimestampAssigner(rowtimeFieldIndex_);
    }

private:
    int32_t rowtimeFieldIndex_ = -1;
    long maxOutOfOrderness_;
};

class MonotonousTimestampsStrategy : public WatermarkStrategy {
public:
    WatermarkGenerator* CreateWatermarkGenerator() override {
        return new AscendingTimestampsWatermarks();
    }
};
