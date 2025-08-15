#include "RowDataUtil.h"

bool RowDataUtil::isAccumulateMsg(RowKind kind) {
    return kind == RowKind::INSERT || kind == RowKind::UPDATE_AFTER;
}

bool RowDataUtil::isRetractMsg(RowKind kind) {
    return kind == RowKind::UPDATE_BEFORE || kind == RowKind::DELETE;
}
