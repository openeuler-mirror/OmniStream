/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_RECORDCOUNTER_H
#define FLINK_TNEL_RECORDCOUNTER_H

#include "table/data/RowData.h"

class RecordCounter {
public:
        virtual ~RecordCounter() = default;
        virtual bool recordCountIsZero(RowData *acc) = 0;
        
        static std::unique_ptr<RecordCounter> of(int indexOfCountStar_);

protected:
        RecordCounter() = default;
};

class AccumulationRecordCounter : public RecordCounter {
public:
        bool recordCountIsZero(RowData *acc) override;
};

class RetractionRecordCounter : public RecordCounter {
public:
        explicit RetractionRecordCounter(int indexOfCountStar_);

        bool recordCountIsZero(RowData *acc) override;

private:
        int indexOfCountStar;
};

#endif // FLINK_TNEL_RECORDCOUNTER_H
