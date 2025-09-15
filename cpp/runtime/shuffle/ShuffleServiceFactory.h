/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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


#endif //SHUFFLESERVICEFACTORY_H
