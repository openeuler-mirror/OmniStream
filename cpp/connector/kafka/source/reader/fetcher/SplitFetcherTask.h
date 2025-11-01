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

#ifndef OMNISTREAM_SPLITFETCHERTASK_H
#define OMNISTREAM_SPLITFETCHERTASK_H

#include <string>

class SplitFetcherTask {
public:
    virtual bool Run() = 0;
    virtual void WakeUp() = 0;
    virtual std::string ToString() = 0;
    virtual ~SplitFetcherTask() = default;
};

#endif // OMNISTREAM_SPLITFETCHERTASK_H
