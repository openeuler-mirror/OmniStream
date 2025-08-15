#include <gtest/gtest.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <taskmanager/RuntimeEnvironment.h>
#include "table/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "test/core/operators/OutputTest.h"
#include "table/runtime/operators/window/processor/AbstractWindowAggProcessor.h"
#include "core/operators/StreamOperatorFactory.h"
#include <thread>
#include <chrono>

using json = nlohmann::json;
using namespace omnistream;

std::string Q12description = R"DELIM(
{
	"operators": [
		{
				"output": {
				"kind": "Row",
				"type": [
					{
						"kind": "logical",
						"isNull": true,
						"type": "BIGINT"
					},
					{
						"kind": "logical",
						"isNull": false,
						"precision": 3,
						"type": "TIMESTAMP",
						"timestampKind": 0
					},
					{
						"kind": "logical",
						"isNull": false,
						"precision": 3,
						"type": "TIMESTAMP",
						"timestampKind": 0
					},
					{
						"kind": "logical",
						"isNull": false,
						"precision": 3,
						"type": "TIMESTAMP",
						"timestampKind": 0
					}
				]
			},
			"inputs": [
				{
					"kind": "Row",
					"type": [
						{
							"kind": "logical",
							"isNull": true,
							"type": "BIGINT"
						},
						{
							"kind": "logical",
							"isNull": true,
							"type": "BIGINT"
						}
					]
				}
			],
			"name": "WindowAggregate(window=[TUMBLE(slice_end=[$slice_end], size=[10 s])], select=[MAX(max$0) AS maxprice, start('w$) AS window_start, end('w$) AS window_end])",
			"description": {
				"originDescription": null,
				"inputTypes": [
					"BIGINT",
					"BIGINT"
				],
				"outputTypes": [
					"BIGINT",
					"BIGINT",
					"BIGINT",
					"BIGINT"
				],
				"grouping": [
					0
				],
				"aggInfoList": {
					"aggregateCalls": [
						{
							"name": "COUNT()",
							"aggIndex": 0,
							"aggregationFunction": "Count1AggFunction",
							"argIndexes": [],
							"consumeRetraction": "false",
							"filterArg": -1
						}
					],
					"AccTypes": [
						"BIGINT"
					],
					"aggValueTypes": [
						"BIGINT"
					],
					"indexOfCountStar": -1
				},
				"generateUpdateBefore": false,
				"isWindowAggregate": true,
				"timeAttributeIndex": -1,
				"size": 10000,
				"window": "TUMBLE(size=[10 s])",
				"timeAttributeType": "TIMESTAMP_WITH_LOCAL_TIME_ZONE"
			},
			"id": "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator"
		}
	]
}
)DELIM";


omnistream::VectorBatch* newVectorBatchOneKeyOneValueQ12Test1() {
    auto* vbatch = new omnistream::VectorBatch(9);
    std::vector<int64_t> bids = {2001,2003,2001,2003,2002,2004,2002,2005,2001};
    std::vector<int64_t> timeStamps = {1789000, 1789000, 1789000, 1789000, 1789000, 1788000, 1788000, 1789000, 1789000};


    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(9, bids.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(9, timeStamps.data()));

    vbatch->setRowKind(0, RowKind::INSERT);
    vbatch->setRowKind(1, RowKind::INSERT);
    vbatch->setRowKind(2, RowKind::INSERT);
    vbatch->setRowKind(3, RowKind::INSERT);
    vbatch->setRowKind(4, RowKind::INSERT);
	vbatch->setRowKind(5, RowKind::INSERT);
	vbatch->setRowKind(6, RowKind::INSERT);
	vbatch->setRowKind(7, RowKind::INSERT);
	vbatch->setRowKind(8, RowKind::INSERT);
    return vbatch;
}

TEST(NEXTMARKTESTQ12, DISABLED_COUNTTEST) {
    omnistream::VectorBatch* vbatch = newVectorBatchOneKeyOneValueQ12Test1();

    //Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator";
    json parsedJson = json::parse(Q12description);
    omnistream::OperatorConfig opConfig(
            uniqueName, //uniqueName:
            "GlobalWindowAgg_By_Simple", //Name
            parsedJson["operators"][0]["inputTypes"],
            parsedJson["operators"][0]["outputTypes"],
            parsedJson["operators"][0]["description"]
    );
    auto *output = new BatchOutputTest();
    auto* slicingWindowOperator = dynamic_cast<SlicingWindowOperator<RowData*, int64_t>*>(
            StreamOperatorFactory::createOperatorAndCollector(opConfig, output));
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("InnerJoinOperator", 2, 1, 0)));
    slicingWindowOperator->setup();
    slicingWindowOperator->initializeState(initializer, new LongSerializer());
    slicingWindowOperator->open();
    slicingWindowOperator->processBatch(vbatch);

	std::this_thread::sleep_for(std::chrono::seconds(15));
    std::cout << "==========================================================" << std::endl;
    auto* resultBatch = reinterpret_cast<omnistream::VectorBatch*> (output->getVectorBatch());
    // print VectorBatch
    int rowCount = resultBatch->GetRowCount();
    int colCount = resultBatch->GetVectorCount();
    for (int i = 0; i < rowCount; i++) {
        for (int j = 0; j < colCount; j++) {
            long result = resultBatch->GetValueAt<int64_t>(j, i);
            std::cout << result;
            std::cout << " ";
        }
        std::cout << to_string(resultBatch->getRowKind(i)) << std::endl;
    }
}
