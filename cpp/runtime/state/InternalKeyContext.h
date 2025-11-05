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
#ifndef FLINK_TNEL_INTERNALKEYCONTEXT_H
#define FLINK_TNEL_INTERNALKEYCONTEXT_H
#include "KeyGroupRange.h"

template<typename K>
class InternalKeyContext {
public:
    virtual ~InternalKeyContext() = default;

    virtual K getCurrentKey() = 0;

    virtual int getCurrentKeyGroupIndex() = 0;

    virtual int getNumberOfKeyGroups() = 0;

    virtual void setCurrentKey(K currentKey) = 0;

    virtual void setCurrentKeyGroupIndex(int currentKeyGroupIndex) = 0;

    virtual KeyGroupRange *getKeyGroupRange() = 0;
};

#endif // FLINK_TNEL_INTERNALKEYCONTEXT_H
