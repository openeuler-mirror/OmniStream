/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */


#ifndef FLINK_TNEL_OPERATORCONSTANTS_H
#define FLINK_TNEL_OPERATORCONSTANTS_H
#include <string_view>
namespace omnistream {
    namespace datastream {
        constexpr std::string_view OPERATOR_NAME_MAP = "org.apache.flink.streaming.api.operators.StreamMap";
        constexpr std::string_view OPERATOR_NAME_REDUCE = "org.apache.flink.streaming.api.operators.StreamGroupedReduceOperator";
        constexpr std::string_view OPERATOR_NAME_FILTER = "org.apache.flink.streaming.api.operators.StreamFilter";
        constexpr std::string_view OPERATOR_NAME_ADDSOURCE = "org.apache.flink.streaming.api.operators.StreamSource";
        constexpr std::string_view OPERATOR_NAME_FROMSOURCE = "org.apache.flink.streaming.api.operators.SourceOperator";
        constexpr std::string_view OPERATOR_NAME_SINK_WRITER = "org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator";
        constexpr std::string_view OPERATOR_NAME_COMMIT_OPERATOR = "org.apache.flink.streaming.runtime.operators.sink.CommitterOperator";
        constexpr std::string_view OPERATOR_NAME_FLATMAP = "org.apache.flink.streaming.api.operators.StreamFlatMap";
        constexpr std::string_view OPERATOR_NAME_GROUP_AGG = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
    }
}

#endif //FLINK_TNEL_OPERATORCONSTANTS_H
