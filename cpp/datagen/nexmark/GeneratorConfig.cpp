/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "GeneratorConfig.h"
std::vector<GeneratorConfig> GeneratorConfig::split(int n) const
{
    std::vector<GeneratorConfig> results;
    if (n == 1) {
        // No split required.
        results.push_back(*this);
    } else {
        int64_t subMaxEvents = maxEvents / n;
        int64_t subFirstEventId = firstEventId;
        for (int i = 0; i < n; i++) {
            if (i == n - 1) {
                // Don't loose any events to round-down.
                subMaxEvents = maxEvents - subMaxEvents * (n - 1);
            }
            results.push_back(copyWithConfig(subFirstEventId, subMaxEvents, firstEventNumber));
            subFirstEventId += subMaxEvents;
        }
    }
    return results;
}
/** Return an estimate of the bytes needed by {@code numEvents}. */
int64_t GeneratorConfig::estimatedBytesForEvents(int64_t numEvents) const
{
    int64_t numPersons = (numEvents * personProportion) / totalProportion;
    int64_t numAuctions = (numEvents * auctionProportion) / totalProportion;
    int64_t numBids = (numEvents * bidProportion) / totalProportion;
    return numPersons * configuration.avgPersonByteSize
           + numAuctions * configuration.avgAuctionByteSize
           + numBids * configuration.avgBidByteSize;
}