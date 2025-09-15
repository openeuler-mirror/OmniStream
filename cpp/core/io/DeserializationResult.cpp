/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include <sstream>
#include "DeserializationResult.h"

std::string DeserializationResult::toDebugString() const
{
    std::ostringstream ss;

    ss << "DeserializationResult value is: whetherFullRecord " << std::boolalpha << whetherFullRecord;
    ss << ", whetherBufferConsumed " << std::boolalpha << whetherBufferConsumed;
    std::string str = ss.str();
    return str;
}

DeserializationResult DeserializationResult_PARTIAL_RECORD(false, true);
DeserializationResult DeserializationResult_INTERMEDIATE_RECORD_FROM_BUFFER (true, false);
DeserializationResult DeserializationResult_LAST_RECORD_FROM_BUFFER(true, true);