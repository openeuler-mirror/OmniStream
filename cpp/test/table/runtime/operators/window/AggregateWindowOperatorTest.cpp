#include <gtest/gtest.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <test/util/test_util.h>

#include "runtime/taskmanager/RuntimeEnvironment.h"
#include "table/runtime/operators/window/AggregateWindowOperator.h"
#include "runtime/operators/window/assigners/SessionWindowAssigner.h"
#include "runtime/operators/window/TimeWindow.h"
#include "core/graph/OperatorConfig.h"
#include "test/core/operators/OutputTest.h"
#include "operators/StreamOperatorFactory.h"

using json = nlohmann::json;

std::string nexmarkQ12Description = R"DELIM({
	"partition": {
		"partitionName": "none",
		"channelNumber": 1
	},
	"operators": [{
		"output": {
			"kind": "Row",
			"type": [{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			},
			{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			}]
		},
		"inputs": [{
			"kind": "Row",
			"type": [{
				"kind": "logical",
				"isNull": true,
				"type": "BIGINT"
			},
			{
				"kind": "logical",
				"isNull": true,
				"precision": 3,
				"type": "TIMESTAMP",
				"timestampKind": 1
			}]
		}],
		"name": "GroupWindowAggregate(groupBy=[bidder], window=[SessionGroupWindow('w$, dateTime, 10000)], properties=[w$start, w$end, w$rowtime, w$proctime], select=[bidder, COUNT(*) AS bid_count, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime])",
		"description": {
                "originDescription": null,
                "inputTypes": ["BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"],
                "outputTypes": ["BIGINT", "BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITH_LOCAL_TIME_ZONE"],
                "windowPropertyTypes": ["TIMESTAMP(3) NOT NULL", "TIMESTAMP(3) NOT NULL", "TIMESTAMP(3) *ROWTIME*", "TIMESTAMP_LTZ(3) *PROCTIME*"],
                "grouping": [0],
                "aggInfoList": {
                    "aggregateCalls": [{
                        "name": "COUNT()",
                        "aggregationFunction": "Count1AggFunction",
                        "argIndexes": [],
                        "consumeRetraction": "false",
                        "filterArg": -1
                    }],
                    "AccTypes": ["BIGINT"],
                    "aggValueTypes": ["BIGINT"],
                    "indexOfCountStar": -1
                },
                "generateUpdateBefore": false,
                "allowedLateness": 0,
                "windowType": "SessionGroupWindow('w$, dateTime, 10000)",
                "countType": "time",
                "timeType": "event",
                "actualSize": 10000,
                "inputTimeFieldIndex": 1
            },
		"id": "org.apache.flink.table.runtime.operators.window.AggregateWindowOperator"
	}]
})DELIM";

omnistream::VectorBatch *Q12VectorBatchInput()
{
//    // 时间窗口相同、bidder id相同: OK
//    auto *vbatch = new omnistream::VectorBatch(3);
//    std::vector<int64_t> bidder = {1, 1, 1};
//    std::vector<int64_t> timeStamps = {
//        1742625623598, 1742625623598, 1742625623598
//    };

    // 时间窗口重叠、bidder id相同
    auto *vbatch = new omnistream::VectorBatch(5);
    std::vector<int64_t> bidder = {1, 1, 2, 1, 2};
    std::vector<int64_t> timeStamps = {
            1742625600000, 1742625605000, 1742625600000, 1742625612000, 1742625800000
    };

    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, bidder.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, timeStamps.data()));

    vbatch->setRowKind(0, RowKind::INSERT);
    vbatch->setRowKind(1, RowKind::INSERT);
    vbatch->setRowKind(2, RowKind::INSERT);
    vbatch->setRowKind(3, RowKind::INSERT);
    vbatch->setRowKind(4, RowKind::INSERT);
    return vbatch;
}

TEST(AggregateWindowOperatorTest, WindowStateTest)
{
    //Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.window.AggregateWindowOperator";
    json parsedJson = json::parse(nexmarkQ12Description);
    omnistream::OperatorConfig opConfig(
        uniqueName, //uniqueName:
        "LocalWindowAgg_By_Simple", //Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]
    );

    auto *output = new BatchOutputTest();
    auto *windowAggOperator = dynamic_cast<AggregateWindowOperator<RowData *, TimeWindow> *>(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(opConfig, output));
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("AggregateWindowOperator", 128, 1, 0)));
    windowAggOperator->initializeState(initializer, new LongSerializer());
    windowAggOperator->open();
    // KeyRowData 10, TimeWindow(1742625620000, 1742625630000), Value 1
    BinaryRowData *bidIdRow = BinaryRowData::createBinaryRowDataWithMem(1);
    bidIdRow->setLong(0, 10);
    TimeWindow timeWindow(1742625620000, 1742625630000);
    windowAggOperator->setCurrentKey(bidIdRow);
    windowAggOperator->windowState->setCurrentNamespace(timeWindow);
    BinaryRowData *countRow = BinaryRowData::createBinaryRowDataWithMem(1);
    countRow->setLong(0, 1);
    windowAggOperator->windowState->update(countRow);
    RowData *windowResult = windowAggOperator->windowState->value();
    ASSERT_EQ(windowResult->getArity(), 1);
    ASSERT_EQ(*windowResult->getLong(0), 1);
    // update value 2. KeyRowData 10, TimeWindow(1742625620000, 1742625630000), Value 2
    BinaryRowData *countRow1 = BinaryRowData::createBinaryRowDataWithMem(1);
    countRow1->setLong(0, 2);
    windowAggOperator->windowState->update(countRow1);
    windowResult = windowAggOperator->windowState->value();
    ASSERT_EQ(windowResult->getArity(), 1);
    ASSERT_EQ(*windowResult->getLong(0), 2);
    // update KeyRowData 20. KeyRowData 20, TimeWindow(1742625620000, 1742625630000), Value 2
    bidIdRow = BinaryRowData::createBinaryRowDataWithMem(1);
    bidIdRow->setLong(0, 20);
    windowAggOperator->setCurrentKey(bidIdRow);
    windowResult = windowAggOperator->windowState->value();
    ASSERT_EQ(windowResult, nullptr);
    countRow = BinaryRowData::createBinaryRowDataWithMem(1);
    countRow->setLong(0, 2);
    windowAggOperator->windowState->update(countRow);
    windowResult = windowAggOperator->windowState->value();
    ASSERT_EQ(windowResult->getArity(), 1);
    ASSERT_EQ(*windowResult->getLong(0), 2);
    // update TimeWindow(1742625630000, 1742625640000).
    timeWindow = TimeWindow(1742625630000, 1742625640000);
    windowAggOperator->windowState->setCurrentNamespace(timeWindow);
    windowResult = windowAggOperator->windowState->value();
    ASSERT_EQ(windowResult, nullptr);
    countRow = BinaryRowData::createBinaryRowDataWithMem(1);
    countRow->setLong(0, 2);
    windowAggOperator->windowState->update(countRow);
    windowResult = windowAggOperator->windowState->value();
    ASSERT_EQ(windowResult->getArity(), 1);
    ASSERT_EQ(*windowResult->getLong(0), 2);
}

TEST(AggregateWindowOperatorTest, TimeWindowTest)
{
    //Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.window.AggregateWindowOperator";
    json parsedJson = json::parse(nexmarkQ12Description);
    omnistream::OperatorConfig opConfig(
        uniqueName, //uniqueName:
        "LocalWindowAgg_By_Simple", //Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]
    );

    auto *output = new BatchOutputTest();
    auto *windowAggOperator = dynamic_cast<AggregateWindowOperator<RowData *, TimeWindow> *>(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(opConfig, output));
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("AggregateWindowOperator", 128, 1, 0)));
    windowAggOperator->initializeState(initializer, new LongSerializer());
    windowAggOperator->open();

    BinaryRowData *key = BinaryRowData::createBinaryRowDataWithMem(1);
    key->setLong(0, 2);
    windowAggOperator->setCurrentKey(key);
    BinaryRowData *inputRow = BinaryRowData::createBinaryRowDataWithMem(2);
    inputRow->setLong(0, 10);
    const std::vector<TimeWindow> assignStateNamespace = windowAggOperator->windowFunction->AssignStateNamespace(
        inputRow, 1742625633598);
    ASSERT_EQ(assignStateNamespace.size(), 1);
    ASSERT_EQ(assignStateNamespace[0].start, 1742625633598);
    ASSERT_EQ(assignStateNamespace.size(), 1);
    ASSERT_EQ(assignStateNamespace[0].end, 1742625643598);

    BinaryRowData *inputRow1 = BinaryRowData::createBinaryRowDataWithMem(2);
    inputRow->setLong(0, 10);
    const std::vector<TimeWindow> assignStateNamespace1 = windowAggOperator->windowFunction->AssignStateNamespace(
        inputRow1, 1742625634598);
    ASSERT_EQ(assignStateNamespace1.size(), 1);
    ASSERT_EQ(assignStateNamespace1[0].start, 1742625633598);
    ASSERT_EQ(assignStateNamespace1.size(), 1);
    ASSERT_EQ(assignStateNamespace1[0].end, 1742625643598);
}

TEST(AggregateWindowOperatorTest, JsonTest)
{
    //Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.window.AggregateWindowOperator";
    json parsedJson = json::parse(nexmarkQ12Description);
    omnistream::OperatorConfig opConfig(
        uniqueName, //uniqueName:
        "LocalWindowAgg_By_Simple", //Name
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]
    );

    auto *output = new BatchOutputTest();
    auto *windowAggOperator = dynamic_cast<AggregateWindowOperator<RowData *, TimeWindow> *>(
        omnistream::StreamOperatorFactory::createOperatorAndCollector(opConfig, output));
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("AggregateWindowOperator", 128, 1, 0)));
    windowAggOperator->initializeState(initializer, new LongSerializer());
    windowAggOperator->open();
    omnistream::VectorBatch *vBatch = Q12VectorBatchInput();
    auto *streamRecord = new StreamRecord(vBatch);
    windowAggOperator->processBatch(streamRecord);
    windowAggOperator->internalTimerService->advanceWatermark(2842625623598);

//    std::cout << "=========== print result ==========" << std::endl;
//    auto *resultBatch = (output->getVectorBatch());
//    // print VectorBatch
//    int rowCount = resultBatch->GetRowCount();
//    int colCount = resultBatch->GetVectorCount();
//    for (int i = 0; i < rowCount; i++) {
//        for (int j = 0; j < colCount; j++) {
//            long result = resultBatch->GetValueAt<int64_t>(j, i);
//            std::cout << result;
//            std::cout << " ";
//        }
//        std::cout << to_string(resultBatch->getRowKind(i)) << std::endl;
//    }
//    std::cout << "LocalWindowAggTest  NexmarkQ12Test1" << std::endl;
}
