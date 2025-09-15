/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef RESCALEMAPPINGS_H
#define RESCALEMAPPINGS_H
#include <vector>
#include <algorithm>
#include <set>
#include <bitset>
#include <iostream>
#include <llvm/Support/CommandLine.h>

#include "IntArrayList.h"

namespace omnistream {

    class IdentityRescaleMappings;
    class RescaleMappings {
    public:
        virtual ~RescaleMappings() = default;

        static RescaleMappings SYMMETRIC_IDENTITY;

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

        virtual std::set<int> getAmbiguousTargets()
        {
            std::set<int> ambiguousTargets;
            std::vector<bool> usedTargets(numberOfTargets, false);

            for (const auto& targets : mappings) {
                for (int target : targets) {
                    if (usedTargets[target]) {
                        ambiguousTargets.insert(target);
                    } else {
                        usedTargets[target] = true;
                    }
                }
            }
            return ambiguousTargets;
        }

        static RescaleMappings of(const std::vector<std::vector<int> > &mappedTargets, int numberOfTargets);

    protected:
        int numberOfSources;
        std::vector<std::vector<int> > mappings;
        int numberOfTargets;

    private:
        static bool isIdentity(const std::vector<std::vector<int> > &mappings, size_t numberOfTargets)
        {
            if (mappings.size() < numberOfTargets) return false;
            for (auto source = numberOfTargets; source < mappings.size(); ++source) {
                if (!mappings[source].empty()) return false;
            }
            for (int source = 0; source < static_cast<int>(numberOfTargets); ++source) {
                if (mappings[source].size() != 1 || source != mappings[source][0]) return false;
            }
            return true;
        }

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

#endif //RESCALEMAPPINGS_H
