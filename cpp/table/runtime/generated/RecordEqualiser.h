#ifndef FLINK_TNEL_RECORD_EQUALISER_H
#define FLINK_TNEL_RECORD_EQUALISER_H

#include "../../data/RowData.h"

class RecordEqualiser {
public:
    virtual ~RecordEqualiser();
    virtual bool equals(RowData* row1, RowData* row2) = 0;
};

#endif  // FLINK_TNEL_RECORD_EQUALISER_H