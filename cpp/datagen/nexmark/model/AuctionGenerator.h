/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_AUCTIONGENERATOR_H
#define OMNISTREAM_AUCTIONGENERATOR_H

#include "EventGenerator.h"
#include "PersonGenerator.h"
// AuctionGenerator class definition.
class AuctionGenerator {
public:
    AuctionGenerator() : random(0)
    {
        nameBuffer = new char[21];
        descBuffer = new char[101];
        extraBuffer = new char[1024];
    }
    /**
 * Generate and return a random auction with next available id.
 */
    std::unique_ptr<Auction> nextAuction(long eventsCountSoFar, long eventId, long timestamp, const GeneratorConfig &config);

    /**
     * Return the last valid auction id (ignoring FIRST_AUCTION_ID). Will be the current auction id if
     * due to generate an auction.
     */
    static long lastBase0AuctionId(const GeneratorConfig &config, long eventId);

    /** Return a random auction id (base 0). */
    static long nextBase0AuctionId(long nextEventId, SplittableRandom &random, const GeneratorConfig &config);

    /** Return a random time delay, in milliseconds, for length of auctions. */
    static long nextAuctionLengthMs(long eventsCountSoFar, SplittableRandom &random, long timestamp, const GeneratorConfig &config);
private:
    SplittableRandom random;
    /**
     * Keep the number of categories small so the example queries will find results even with a small
     * batch of events.
     */
    static const int NUM_CATEGORIES = 5;

    /** Number of yet-to-be-created people and auction ids allowed. */
    static const int AUCTION_ID_LEAD = 10;

    /**
     * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these
     * values.
     */
    static const int HOT_SELLER_RATIO = 100;

    char* nameBuffer;
    char* descBuffer;
    char* extraBuffer;
};


#endif // OMNISTREAM_AUCTIONGENERATOR_H
