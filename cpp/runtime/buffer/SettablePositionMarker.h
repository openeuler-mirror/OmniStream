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
#include <iostream>
// #include <cmath>    // For abs function for floating-point numbers
#include <cstdlib> // For abs function for integer types
#include <climits> // For INT_MIN

/***
class SettablePositionMarker
{
public:
    int get() const;
    bool isFinished() const;
    int getCached() const;

    int markFinished();

    void move(int offset);
    void set(int value);

    void commit();

private:
    volatile int position = 0;
    int cachedPosition = 0;
};

**/