/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <string>

enum class RestoreSavepointMode {
    OMNI_INTERNAL,
    FLINK_COMPATIBLE
};

enum class FlinkSavepointAdaptorType {
    None,
    OmniIsCompatible,
    DeduplicateAdaptor,
    AppendOnlyTopNAdaptor,
    StreamingJoinNoUniqueKeyAdaptor,
    StreamingLeftOuterJoinNoUniqueKeyAdaptor,
    WindowJoinAdaptor,
    GroupAggAdaptor,
    GroupWindowAggAdaptor
};

inline const char* flinkSavepointAdaptorTypeName(FlinkSavepointAdaptorType type) noexcept
{
    switch (type) {
        case FlinkSavepointAdaptorType::None: return "None";
        case FlinkSavepointAdaptorType::OmniIsCompatible: return "OmniIsCompatible";
        case FlinkSavepointAdaptorType::DeduplicateAdaptor: return "DeduplicateAdaptor";
        case FlinkSavepointAdaptorType::AppendOnlyTopNAdaptor: return "AppendOnlyTopNAdaptor";
        case FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor: return "StreamingJoinNoUniqueKeyAdaptor";
        case FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor:
            return "StreamingLeftOuterJoinNoUniqueKeyAdaptor";
        case FlinkSavepointAdaptorType::WindowJoinAdaptor: return "WindowJoinAdaptor";
        case FlinkSavepointAdaptorType::GroupAggAdaptor: return "GroupAggAdaptor";
        case FlinkSavepointAdaptorType::GroupWindowAggAdaptor: return "GroupWindowAggAdaptor";
        default: return "Unknown";
    }
}

struct FlinkSavepointAdaptorInfo {
    FlinkSavepointAdaptorType type = FlinkSavepointAdaptorType::None;
    std::string reason;
};
