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

#ifndef RESCALEMAPPINGS_H
#define RESCALEMAPPINGS_H
#include <vector>
#include <algorithm>
#include <set>
#include <bitset>
#include <sstream>
#include <iostream>
#include <llvm/Support/CommandLine.h>

#include "IntArrayList.h"

namespace omnistream {

    class IdentityRescaleMappings;
    class RescaleMappings {
    public:
        virtual ~RescaleMappings() = default;

        static std::shared_ptr<RescaleMappings> SYMMETRIC_IDENTITY;

        static constexpr int EMPTY_TARGETS[] = {};

        RescaleMappings(int numberOfSources, std::vector<std::vector<int> > mappings, int numberOfTargets)
            : numberOfSources(numberOfSources), mappings(std::move(mappings)), numberOfTargets(numberOfTargets) {
        }

        virtual bool isIdentity() const { return false; }

        std::vector<int> getMappedIndexes(size_t sourceIndex) const
        {
            if (sourceIndex >= mappings.size()) {
                return {};
            }
            return mappings[sourceIndex];
        }

        bool operator==(const RescaleMappings &other) const
        {
            return this == &other || (numberOfSources == other.numberOfSources &&
                                      numberOfTargets == other.numberOfTargets &&
                                      mappings == other.mappings);
        }

        size_t hashCode() const
        {
            size_t hash = 0;
            for (const auto &mapping: mappings) {
                for (int index: mapping) {
                    hash ^= std::hash<int>()(index) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
                }
            }
            return hash;
        }

        virtual std::set<int> getAmbiguousTargets();

        static RescaleMappings of(const std::vector<std::vector<int> > &mappedTargets, int numberOfTargets);
        std::string ToString() const;
        RescaleMappings invert();
    protected:
        int numberOfSources;
        std::vector<std::vector<int> > mappings;
        int numberOfTargets;

    private:
        static bool isIdentity(const std::vector<std::vector<int> > &mappings_, size_t numberOfTargets_);

        static std::vector<int> toSortedArray(IntArrayList sourceList)
        {
            std::vector<int> sources = sourceList.toArray();
            std::sort(sources.begin(), sources.end());
            return sources;
        }
    };


    class IdentityRescaleMappings : public RescaleMappings {
    public:
        static constexpr int IMPLICIT_MAPPING[][1] = {};

        IdentityRescaleMappings(int numberOfSources, int numberOfTargets)
            : RescaleMappings(numberOfSources, {}, numberOfTargets) {}

        bool isIdentity() const override { return true; }

        std::set<int> getAmbiguousTargets() override { return {}; }
    };

}

#endif
