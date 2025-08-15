#include <gtest/gtest.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <taskmanager/RuntimeEnvironment.h>
#include "table/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "test/core/operators/OutputTest.h"
#include "table/runtime/operators/window/processor/AbstractWindowAggProcessor.h"
#include "core/operators/StreamOperatorFactory.h"

using json = nlohmann::json;
using namespace omnistream;

std::string Q7hopdescription = R"DELIM(
{
	"description": {
		"timeAttributeType": "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
		"inputTypes": [
			"BIGINT",
			"BIGINT"
		],
		"aggInfoList": {
			"localAggregateCalls": [
				{
					"name": "MAX($0)",
					"filterArg": -1,
					"argIndexes": [
						0
					],
					"aggregationFunction": "LongMaxAggFunction",
					"consumeRetraction": "false"
				}
			],
			"globalAggValueTypes": [
				"BIGINT"
			],
			"indexOfCountStar": -1,
			"globalAggregateCalls": [
				{
					"name": "MAX($0)",
					"filterArg": -1,
					"argIndexes": [
						0
					],
					"aggregationFunction": "LongMaxAggFunction",
					"consumeRetraction": "false"
				}
			],
			"globalAccTypes": [
				"BIGINT"
			]
		},
		"size": 10000,
		"sliceEndIndex": 1,
		"generateUpdateBefore": false,
		"outputTypes": [
			"BIGINT",
			"TIMESTAMP_WITHOUT_TIME_ZONE(3)",
			"TIMESTAMP_WITHOUT_TIME_ZONE(3)"
		],
		"timeAttributeIndex": 2147483647,
		"window": "TUMBLE(size=[10 s])",
		"grouping": [],
		"originDescription": "[9]:GlobalWindowAggregate(window=[TUMBLE(slice_end=[$slice_end], size=[10 s])], select=[MAX(max$0) AS maxprice, start('w$) AS window_start, end('w$) AS window_end])"
	}
}
)DELIM";


omnistream::VectorBatch* newVectorBatchOneKeyOneValueQ7Test1() {
    auto* vbatch = new omnistream::VectorBatch(3);
    std::vector<int64_t> bids = {59333717, 147347, 65489};
    std::vector<int64_t> timeStamps = {1731973220000, 1731973230000, 1731973230000};


    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(3, bids.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(3, timeStamps.data()));
    for (int i = 0; i < bids.size(); i++) {
        vbatch->setRowKind(i, RowKind::INSERT);
    }
    return vbatch;
}

TEST(NEXTMARKTESTQ7, MAXTEST) {
    omnistream::VectorBatch* vbatch = newVectorBatchOneKeyOneValueQ7Test1();

    //Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator";
    json parsedJson = json::parse(Q7hopdescription);
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

    LOG("step1")
    slicingWindowOperator->open();
    LOG("step2")
    slicingWindowOperator->processBatch(vbatch);
    slicingWindowOperator->ProcessWatermark(new Watermark(INT64_MAX));

    BatchOutputTest* batchOutput = output;
    LOG("step3")
    auto* resultBatch = reinterpret_cast<omnistream::VectorBatch*> (batchOutput->getVectorBatch());
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
