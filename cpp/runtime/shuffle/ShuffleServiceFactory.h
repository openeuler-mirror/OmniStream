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

#ifndef SHUFFLESERVICEFACTORY_H
#define SHUFFLESERVICEFACTORY_H
#include <memory>

#include "ShuffleEnvironment.h"
#include "ShuffleEnvironmentContext.h"


namespace omnistream {
    class ShuffleServiceFactory {
    public:
            virtual std::shared_ptr<ShuffleEnvironment> createOmniShuffleEnvironment(std::shared_ptr<ShuffleEnvironmentContext> context) = 0;
    };
}


#endif
