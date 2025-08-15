/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_TABLEOPERATORCONSTANTS_H
#define FLINK_TNEL_TABLEOPERATORCONSTANTS_H
#include <string_view>
constexpr std::string_view OPERATOR_NAME_STREAM_CALC = "StreamExecCalc";
constexpr std::string_view OPERATOR_NAME_STREAM_EXPAND = "StreamExecExpand";
constexpr std::string_view OPERATOR_NAME_KEYED_PROCESS_OPERATOR =
    "org.apache.flink.streaming.api.operators.KeyedProcessOperator";
constexpr std::string_view OPERATOR_NAME_STREAM_JOIN =
    "org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator";
constexpr std::string_view OPERATOR_NAME_WATERMARK_ASSIGNER =
    "org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperator";
constexpr std::string_view OPERATOR_NAME_WINDOW_INNER_JOIN =
    "org.apache.flink.table.runtime.operators.join.window.WindowJoinOperator.InnerJoinOperator";
constexpr std::string_view OPERATOR_NAME_PROCESS_OPERATOR = "org.apache.flink.streaming.api.operators.ProcessOperator";
constexpr std::string_view OPERATOR_NAME_COLLECT_SINK =
    "org.apache.flink.streaming.api.operators.collect.CollectSinkOperator";
constexpr std::string_view OPERATOR_NAME_SINK = "org.apache.flink.table.runtime.operators.sink.SinkOperator";
constexpr std::string_view OPERATOR_NAME_STREAM_SINK = "org.apache.flink.streaming.api.operators.StreamSink";
constexpr std::string_view OPERATOR_NAME_STREAM_SOURCE = "org.apache.flink.streaming.api.operators.StreamSource";
constexpr std::string_view OPERATOR_NAME_SOURCE_OPERATOR = "org.apache.flink.streaming.api.operators.SourceOperator";
constexpr std::string_view OPERATOR_NAME_LOCAL_WINDOW_AGG =
    "org.apache.flink.table.runtime.operators.aggregate.window.LocalSlicingWindowAggOperator";
constexpr std::string_view OPERATOR_NAME_GLOBAL_WINDOW_AGG =
    "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator";
constexpr std::string_view OPERATOR_NAME_STREAMRECORDTIMESTAMPINSERTER =
    "org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter";
constexpr std::string_view OPERATOR_NAME_WINDOW_AGG =
    "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator";
constexpr std::string_view OPERATOR_NAME_CONSTRAINTENFORCER =
    "org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer";
constexpr std::string_view OPERATOR_NAME_GROUP_WINDOW_AGG =
    "org.apache.flink.table.runtime.operators.window.AggregateWindowOperator";
constexpr std::string_view OPERATOR_NAME_INPUT_CONVERSION =
        "org.apache.flink.table.runtime.operators.source.InputConversionOperator";
constexpr std::string_view OPERATOR_NAME_FILTER = "org.apache.flink.streaming.api.operators.StreamFilter";
constexpr std::string_view OPERATOR_NAME_SINK_WRITER =
"org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator";
constexpr std::string_view OPERATOR_NAME_COMMIT_OPERATOR =
"org.apache.flink.streaming.runtime.operators.sink.CommitterOperator";
constexpr std::string_view OPERATOR_NAME_OUTPUT_CONVERSION =
"org.apache.flink.table.runtime.operators.sink.OutputConversionOperator";
constexpr std::string_view OPERATOR_NAME_STREAMING_FILE_WRITER =
    "org.apache.flink.connector.file.table.stream.StreamingFileWriter";
constexpr std::string_view OPERATOR_NAME_PARTITION_COMMITTER =
    "org.apache.flink.connector.file.table.stream.PartitionCommitter";

#endif  // FLINK_TNEL_TABLEOPERATORCONSTANTS_H
