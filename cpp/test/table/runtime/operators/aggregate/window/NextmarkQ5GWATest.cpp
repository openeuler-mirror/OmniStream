#include <gtest/gtest.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <taskmanager/RuntimeEnvironment.h>

#include "table/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "test/core/operators/OutputTest.h"
#include "table/runtime/operators/window/processor/AbstractWindowAggProcessor.h"
#include "core/operators/StreamOperatorFactory.h"
#include "table/runtime/operators/TimerHeapInternalTimer.h"

using json = nlohmann::json;
using namespace omnistream;

std::string Q5MAXdescription1 = R"DELIM(
{
	"name": "GlobalWindowAggregate[5]",
	"id": "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator",
	"description": {
		"timeAttributeType": "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
		"inputTypes": ["BIGINT", "BIGINT", "BIGINT"
		],
		"aggInfoList": {
			"localAggregateCalls": [
				{
					"name": "COUNT()", "filterArg": -1, "argIndexes": [], "aggregationFunction": "Count1AggFunction",
					"consumeRetraction": "false"
				}
			],
			"globalAggValueTypes": [
				"BIGINT"
			],
			"indexOfCountStar": 0,
			"globalAggregateCalls": [
				{
					"name": "COUNT()", "filterArg": -1, "argIndexes": [], "aggregationFunction": "Count1AggFunction",
					"consumeRetraction": "false"
				}
			],
			"globalAccTypes": [
				"BIGINT"
			]
		},
		"size": 10000, "sliceEndIndex": 2, "slide": 2000, "generateUpdateBefore": false,
		"outputTypes": [
			"BIGINT", "BIGINT", "TIMESTAMP_WITHOUT_TIME_ZONE(3)", "TIMESTAMP_WITHOUT_TIME_ZONE(3)"
		],
		"timeAttributeIndex": 2147483647,
		"window": "HOP(size=[10 s], slide=[2 s])",
		"grouping": [
			0
		],
		"originDescription": "[5]:GlobalWindowAggregate(groupBy=[auction], window=[HOP(slice_end=[$slice_end], size=[10 s], slide=[2 s])], select=[auction, COUNT(count1$0) AS num, start('w$) AS window_start, end('w$) AS window_end])"
	},
	"inputs": [
		{
			"kind": "Row", "isNull": false, "precision": 0,
			"type": [
				{
					"kind": "logical", "isNull": true, "type": "BIGINT"
				},
				{
					"kind": "logical", "isNull": true, "type": "BIGINT"
				},
				{
					"kind": "logical", "isNull": true, "type": "BIGINT"
				}
			],
			"timestampKind": 0,
			"fieldName": ""
		}
	],
	"output": {
		"kind": "Row",
		"isNull": false,
		"precision": 0,
		"type": [
			{
				"kind": "logical", "isNull": true, "type": "BIGINT"
			},
			{
				"kind": "logical", "isNull": false, "type": "BIGINT"
			},
			{
				"kind": "logical", "isNull": false, "precision": 3, "type": "TIMESTAMP", "timestampKind": 0
			},
			{
				"kind": "logical", "isNull": false, "precision": 3, "type": "TIMESTAMP", "timestampKind": 0
			}
		],
		"timestampKind": 0,
		"fieldName": ""
	},
	"vertexID": 5
}
)DELIM";

std::string Q5COUNTdescription1 =R"DELIM(
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
						},
						{
							"kind": "logical",
							"isNull": true,
							"type": "BIGINT"
						}
					]
				}
			],
			"name": "GlobalWindowAggregate(groupBy=[auction], window=[HOP(slice_end=[$slice_end], size=[10 s], slide=[2 s])], select=[auction, COUNT(count1$0) AS num, start('w$) AS window_start, end('w$) AS window_end])",
			"description": {
                "aggInfoList": {
                    "globalAccTypes": [
                        "BIGINT"
                    ],
                    "globalAggValueTypes": [
                        "BIGINT"
                    ],
                    "globalAggregateCalls": [
                        {
                            "aggregationFunction": "Count1AggFunction",
                            "argIndexes": [],
                            "consumeRetraction": "false",
                            "filterArg": -1,
                            "name": "COUNT()"
                        }
                    ],
                    "indexOfCountStar": 0,
                    "localAggregateCalls": [
                        {
                            "aggregationFunction": "Count1AggFunction",
                            "argIndexes": [],
                            "consumeRetraction": "false",
                            "filterArg": -1,
                            "name": "COUNT()"
                        }
                    ]
                },
                "generateUpdateBefore": false,
                "grouping": [
                    0
                ],
                "inputTypes": [
                    "BIGINT",
                    "BIGINT",
                    "BIGINT"
                ],
                "originDescription": "[5]:GlobalWindowAggregate(groupBy=[auction], window=[HOP(slice_end=[$slice_end], size=[10 s], slide=[2 s])], select=[auction, COUNT(count1$0) AS num, start('w$) AS window_start, end('w$) AS window_end])",
                "outputTypes": [
                    "BIGINT",
                    "BIGINT",
                    "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                    "TIMESTAMP_WITHOUT_TIME_ZONE(3)"
                ],
                "size": 10000,
                "sliceEndIndex": 2,
                "slide": 2000,
                "timeAttributeIndex": 2147483647,
                "timeAttributeType": "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
                "window": "HOP(size=[10 s], slide=[2 s])"
            },
			"id": "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator"
		},
		{
			"output": {
				"kind": "Row",
				"type": [
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
						"type": "BIGINT"
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
							"isNull": false,
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
						}
					]
				}
			],
			"name": "Calc(select=[window_start AS starttime, window_end AS endtime, num])",
			"description": {
				"inputTypes": [
					"BIGINT",
					"BIGINT",
					"TIMESTAMP_WITHOUT_TIME_ZONE(3)",
					"TIMESTAMP_WITHOUT_TIME_ZONE(3)"
				],
				"indices": [
					{
						"exprType": "FIELD_REFERENCE",
						"dataTypeId": 14,
						"colVal": 2
					},
					{
						"exprType": "FIELD_REFERENCE",
						"dataTypeId": 14,
						"colVal": 3
					},
					{
						"exprType": "FIELD_REFERENCE",
						"dataTypeId": 9,
						"colVal": 1
					}
				],
				"condition": null,
				"outputTypes": [
					"TIMESTAMP_WITHOUT_TIME_ZONE(3)",
					"TIMESTAMP_WITHOUT_TIME_ZONE(3)",
					"BIGINT"
				],
				"originDescription": null
			},
			"id": "StreamExecCalc"
		}
	]
}
)DELIM";

omnistream::VectorBatch* newVectorBatchOneKeyOneValueQ5Test1() {
    auto* vbatch = new omnistream::VectorBatch(2);
    std::vector<int64_t> auctionId = {1010, 1020};
    std::vector<int64_t> countNum = {1, 1};
    std::vector<int64_t> time = {1731972676000, 1731972676000};

    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(2, auctionId.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(2, countNum.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(2, time.data()));

    vbatch->setRowKind(0, RowKind::INSERT);
    vbatch->setRowKind(1, RowKind::INSERT);
    return vbatch;
}

omnistream::VectorBatch* newVectorBatchOneKeyOneElementQ5Test1() {
    auto* vbatch = new omnistream::VectorBatch(1);
    std::vector<int64_t> auction = {1001};
    std::vector<int64_t> countRes = {1};
    std::vector<int64_t> time = {1731972676000};

    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, auction.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, countRes.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(1, time.data()));

    vbatch->setRowKind(0, RowKind::INSERT);
    return vbatch;
}

omnistream::VectorBatch* newVectorBatchOneKeyOneValueQ5Test2() {
    auto* vbatch = new omnistream::VectorBatch(5);
    std::vector<int64_t> auction = {1010, 1010, 1000, 1000, 599};
    std::vector<int64_t> count = {1, 2, 1, 3, 10};
    std::vector<int64_t> time = {1731943876000, 1731943876000, 1731943878000, 1731943878000, 1731943888000};

    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, auction.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, count.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, time.data()));

    vbatch->setRowKind(0, RowKind::INSERT);
    vbatch->setRowKind(1, RowKind::INSERT);
    vbatch->setRowKind(2, RowKind::INSERT);
    vbatch->setRowKind(3, RowKind::INSERT);
    vbatch->setRowKind(4, RowKind::INSERT);
    return vbatch;
}

TEST(NEXTMARKTESTQ5, MAXTEST) {
omnistream::VectorBatch* vbatch = newVectorBatchOneKeyOneValueQ5Test1();

//Operator description
std::string uniqueName = "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator";
json parsedJson = json::parse(Q5MAXdescription1);
omnistream::OperatorConfig opConfig(
        uniqueName, //uniqueName:
        "GlobalWindowAgg_By_Simple", //Name
        parsedJson["description"]["inputTypes"],
        parsedJson["description"]["outputTypes"],
        parsedJson["description"]
);
auto *output = new BatchOutputTest();
auto* slicingWindowOperator = dynamic_cast<SlicingWindowOperator<RowData*, int64_t>*>(
        StreamOperatorFactory::createOperatorAndCollector(opConfig, output));

StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("GlobalWindowAgg", 128, 1, 0)));
std::vector<RowField> typeInfo{RowField("col0", BasicLogicalType::BIGINT)};

slicingWindowOperator->setup();
TypeSerializer *ser = new RowDataSerializer(new RowType(false, typeInfo));
slicingWindowOperator->initializeState(initializer, ser);


slicingWindowOperator->open();
slicingWindowOperator->processBatch(vbatch);
slicingWindowOperator->ProcessWatermark(new Watermark(INT64_MAX));

omnistream::VectorBatch* resultBatch = reinterpret_cast<omnistream::VectorBatch*> (output->getVectorBatch());
// print VectorBatch
int32_t rowCount = resultBatch->GetRowCount();
int32_t colCount = resultBatch->GetVectorCount();
for (int i = 0; i < rowCount; i++) {
for (int j = 0; j < colCount; j++) {
long result = resultBatch->GetValueAt<int64_t>(j, i);
std::cout << result;
std::cout << " ";
}
std::cout << to_string(resultBatch->getRowKind(i)) << std::endl;
}
std::cout << "Global WindowAggTest  Q5MAXTEST" << std::endl;
}

TEST(NEXTMARKTESTQ5, DISABLED_COUNTTEST) {
omnistream::VectorBatch* vbatch = newVectorBatchOneKeyOneElementQ5Test1();

//Operator description
std::string uniqueName = "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator";
json parsedJson = json::parse(Q5COUNTdescription1);
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
slicingWindowOperator->ProcessWatermark(new Watermark(INT64_MAX));
//    BatchOutputTest* batchOutput = dynamic_cast<BatchOutputTest*>(slicingWindowOperator->getOutput());
//    auto *timer = new TimerHeapInternalTimer<RowData*, int64_t>(-1, new BinaryRowData(0), 1010);
//    slicingWindowOperator->onTimer(timer);

omnistream::VectorBatch* resultBatch = reinterpret_cast<omnistream::VectorBatch*> (output->getVectorBatch());
// print VectorBatch
int32_t rowCount = resultBatch->GetRowCount();
int32_t colCount = resultBatch->GetVectorCount();
for (int i = 0; i < rowCount; i++) {
for (int j = 0; j < colCount; j++) {
long result = resultBatch->GetValueAt<int64_t>(j, i);
std::cout << result;
std::cout << " ";
}
std::cout << to_string(resultBatch->getRowKind(i)) << std::endl;
}
std::cout << "Global WindowAggTest  COUNTTEST" << std::endl;
}


