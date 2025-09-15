/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "RescaleMappings.h"

namespace omnistream {

    RescaleMappings RescaleMappings::of(const std::vector<std::vector<int> > &mappedTargets, int numberOfTargets)
    {
        std::vector<std::vector<int> > mappings;
        for (const auto &targets: mappedTargets) {
            mappings.push_back(targets.empty() ? std::vector<int>{} : targets);
        }

        if (isIdentity(mappings, numberOfTargets)) {
            return IdentityRescaleMappings(mappings.size(), numberOfTargets);
        }

        int lastNonEmpty = mappings.size() - 1;
        while (lastNonEmpty >= 0 && mappings[lastNonEmpty].empty()) --lastNonEmpty;

        size_t length = lastNonEmpty + 1;
        return RescaleMappings(
            mappings.size(),
            length == mappings.size()
                ? mappings
                : std::vector<std::vector<int> >(mappings.begin(), mappings.begin() + length),
            numberOfTargets);
    }

}