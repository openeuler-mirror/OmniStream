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

#ifndef OMNISTREAM_INTERNALTIMER_H
#define OMNISTREAM_INTERNALTIMER_H
/**
 * K: such as Object*
 * N: such as VoidNamespace*
 * */
#include <iostream>

template<typename K, typename N>
class InternalTimer {
public:
    virtual ~InternalTimer() = default;
    virtual int64_t getTimestamp() = 0;
    virtual K getKey() = 0;
    virtual N getNamespace() = 0;
};

#endif // OMNISTREAM_INTERNALTIMER_H
