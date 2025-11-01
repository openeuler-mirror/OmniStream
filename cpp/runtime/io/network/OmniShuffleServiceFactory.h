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

#ifndef OMNISHUFFLESERVICEFACTORY_H
#define OMNISHUFFLESERVICEFACTORY_H

#include <partition/ResultPartitionManager.h>
#include <taskmanager/OmniShuffleEnvironmentConfiguration.h>

#include "shuffle/ShuffleEnvironment.h"
#include "shuffle/ShuffleServiceFactory.h"
#include "io/network/ConnectionManager.h"

namespace omnistream {
    class OmniShuffleServiceFactory : public ShuffleServiceFactory {
    public:
        OmniShuffleServiceFactory() = default;
        virtual ~OmniShuffleServiceFactory() = default;
        std::shared_ptr<ShuffleEnvironment> createOmniShuffleEnvironmentWithContext(
            std::shared_ptr<ShuffleEnvironmentContext> context);

        std::shared_ptr<ShuffleEnvironment> createOmniShuffleEnvironment(
            std::shared_ptr<ShuffleEnvironmentContext> context) override;

    private:
        static std::shared_ptr<ShuffleEnvironment> createOmniShuffleEnvironmentWithConfig(
            std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
            ResourceIDPOD resourceId);
        static std::shared_ptr<ShuffleEnvironment> createOmniShuffleEnvironmentWithResultPartitionMgr(
            std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
            ResourceIDPOD resourceId,
            std::shared_ptr<ResultPartitionManager>);

        /* full arg and logic factory method */
        static std::shared_ptr<ShuffleEnvironment> createOmniShuffleEnvironmentWithConnectionMgr(
            std::shared_ptr<OmniShuffleEnvironmentConfiguration> config,
            ResourceIDPOD resourceId,
            std::shared_ptr<ResultPartitionManager>,
            std::shared_ptr<ConnectionManager>);
    };
}


#endif
