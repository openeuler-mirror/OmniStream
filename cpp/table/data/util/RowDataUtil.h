#ifndef FLINK_TNEL_ROW_DATA_UTIL_H
#define FLINK_TNEL_ROW_DATA_UTIL_H

#include "../RowData.h"
#include "../../../table/RowKind.h"

class RowDataUtil {
public:
    static bool isAccumulateMsg(RowKind row);

    static bool isRetractMsg(RowKind row);
};

#endif // FLINK_TNEL_ROW_DATA_UTIL_H
