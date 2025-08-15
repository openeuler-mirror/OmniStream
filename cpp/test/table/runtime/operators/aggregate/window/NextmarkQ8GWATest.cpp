#include <gtest/gtest.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <taskmanager/RuntimeEnvironment.h>
#include "table/vectorbatch/VectorBatch.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "test/core/operators/OutputTest.h"
#include "table/runtime/operators/window/processor/AbstractWindowAggProcessor.h"
#include "core/operators/StreamOperatorFactory.h"
#include <vector>

using json = nlohmann::json;
using namespace omnistream;

std::string Q8hopdescription = R"DELIM({
	"partition": {
		"partitionName": "hash",
		"channelNumber": 1,
		"hashFields": [
			{
				"fieldIndex": 0,
				"fieldName": "f0",
				"fieldTypeName": "BIGINT"
			}
		]
	},
	"operators": [
		{
			"description": {
				"timeAttributeType": "TIMESTAMP_WITHOUT_TIME_ZONE(3)",
				"inputTypes": [
					"BIGINT",
					"VARCHAR(2147483647)",
					"BIGINT"
				],
				"window": "TUMBLE(size=[10 s])",
				"timeAttributeIndex": 2,
				"aggInfoList": {
					"localAggregateCalls": [],
					"globalAggValueTypes": [],
					"indexOfCountStar": -1,
					"globalAggregateCalls": [],
					"globalAccTypes": []
				},
				"generateUpdateBefore": false,
				"outputTypes": [
					"BIGINT",
					"VARCHAR(2147483647)",
					"BIGINT",
					"BIGINT"
				],
				"grouping": [
					0,
					1
				],
				"originDescription": null
			},
			"id": "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator"
		}
	]
})DELIM";


omnistream::VectorBatch* newVectorBatchOneKeyOneValueQ8Test1() {
	auto* vbatch = new omnistream::VectorBatch(5);
	std::vector<int64_t> id = {1, 1, 1, 15, 1};
	std::vector<std::string> name = {"A", "A", "B", "C", "D"}; // 待修改为string类型
	//    std::vector<int64_t> name = {10, 10, 30, 40, 10};
	std::vector<int64_t> time = {1000, 1000, 1000, 1000, 10000};

	vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, id.data()));
	//    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, name.data()));
	vbatch->Append(omniruntime::TestUtil::CreateVarcharVector(name.data(), 5));
	vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(5, time.data()));

	vbatch->setRowKind(0, RowKind::INSERT);
	vbatch->setRowKind(1, RowKind::INSERT);
	vbatch->setRowKind(2, RowKind::INSERT);
	vbatch->setRowKind(3, RowKind::INSERT);
	vbatch->setRowKind(4, RowKind::INSERT);
	return vbatch;
}

TEST(NEXTMARKTESTQ8, DISABLED_EMPTY) {
    omnistream::VectorBatch* vbatch = newVectorBatchOneKeyOneValueQ8Test1();

    //Operator description
    std::string uniqueName = "org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator";
    json parsedJson = json::parse(Q8hopdescription);
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

    std::cout << "================step 1 in Q8===================" << std::endl;
    slicingWindowOperator->open();
    std::cout << "================step 2 in Q8===================" << std::endl;
    slicingWindowOperator->processBatch(vbatch);
    slicingWindowOperator->ProcessWatermark(new Watermark(-1));
//    BatchOutputTest* batchOutput = dynamic_cast<BatchOutputTest*>(slicingWindowOperator->getOutput());

    slicingWindowOperator->processBatch(vbatch);
    std::cout << "================step 5 in nextmark Q8===================" << std::endl;
    slicingWindowOperator->ProcessWatermark(new Watermark(-1));
    std::cout << "================step 6 in nextmark Q8===================" << std::endl;
    BinaryRowData* binaryRowData = BinaryRowData::createBinaryRowDataWithMem(2);
    binaryRowData->setLong(0, 1);
    std::string_view str = "A";
    binaryRowData->setStringView(1, str);
    auto *timer = new TimerHeapInternalTimer<RowData*, int64_t>(-1, binaryRowData, 1000); // todo 传入10时也有结果
    slicingWindowOperator->onTimer(timer);
//    batchOutput = dynamic_cast<BatchOutputTest*>(slicingWindowOperator->getOutput());
	std::cout << "=========== print result ==========" << std::endl;
	auto* resultBatch = reinterpret_cast<omnistream::VectorBatch*> (output->getVectorBatch());
	// print VectorBatch
	std::vector types = {"BIGINT","VARCHAR","BIGINT","BIGINT"};
	int rowCount = resultBatch->GetRowCount();
	int colCount = resultBatch->GetVectorCount();
	for (int i = 0; i < rowCount; i++) {
		for (int j = 0; j < colCount; j++) {
			if (types[j] == "VARCHAR") {
				auto result = reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>> *>(resultBatch->Get(j))->GetValue(i);
				std::string resStr(result);
				std::cout << resStr;
			} else if (types[j] == "INTEGER") {
				int result = resultBatch->GetValueAt<int32_t>(j, i);
				std::cout << result;
			} else if (types[j] == "BIGINT") {
				long result = resultBatch->GetValueAt<int64_t>(j, i);
				std::cout << result;
			} else {
				std::string result = "NNNNO";
				std::cout << result;
			}
			std::cout << " ";
		}
		std::cout << to_string(resultBatch->getRowKind(i)) << std::endl;
	}
}