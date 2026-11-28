/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd
 * OmniStream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "NexmarkUtils.h"
// Definition of static members of RateUnit.
const NexmarkUtils::RateUnit NexmarkUtils::RateUnit::PER_SECOND(1000000L);
const NexmarkUtils::RateUnit NexmarkUtils::RateUnit::PER_MINUTE(60000000L);

// Definition of static instances of RateShape.
const NexmarkUtils::RateShape NexmarkUtils::RateShape::SQUARE_SHAPE(NexmarkUtils::RateShape::SQUARE);
const NexmarkUtils::RateShape NexmarkUtils::RateShape::SINE_SHAPE(NexmarkUtils::RateShape::SINE);
