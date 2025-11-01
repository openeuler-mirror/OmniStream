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

#include "thirdlibrary/java_util_Random.h"
Random::Random() = default;
Random::~Random() = default;

int Random::nextInt(int i)
{
    std::uniform_int_distribution<int> rand_int_generator(0, i);
    std::default_random_engine engine(std::random_device{} ());
    return rand_int_generator(engine);
}

double Random::nextDouble()
{
    std::uniform_real_distribution<double> rand_double_generator(0, 1);
    std::default_random_engine engine(std::random_device{} ());
    return rand_double_generator(engine);
}
