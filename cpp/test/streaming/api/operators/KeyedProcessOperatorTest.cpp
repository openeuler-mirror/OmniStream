#include <gtest/gtest.h>
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "core/streamrecord/StreamRecord.h"
#include "core/operators/TimestampedCollector.h"
#include "table/runtime/operators/aggregate/GroupAggFunction.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/data/RowData.h"
#include <nlohmann/json.hpp>
#include "runtime/taskmanager/RuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include <vector>
#include <test/util/test_util.h>
#include "test/core/operators/OutputTest.h"
#include "runtime/operators/rank/AppendOnlyTopNFunction.h"

using json = nlohmann::json;

class MockUserFunction : public KeyedProcessFunction<RowData *, RowData *, RowData *>
{
public:
    void open(const Configuration& parameters) override {};
    void processElement(RowData &input, Context &ctx, TimestampedCollector &out) override { isCalled = true; }
    bool isCalled = false;
    JoinedRowData* getResultRow() override {return nullptr;};
    void processBatch(omnistream::VectorBatch* inputBatch, Context& ctx, TimestampedCollector& out) override {};
    ValueState<RowData*>* getValueState() override {return nullptr;};
};

TEST(KeyedProcessOperatorTest, Constructor)
{

    std::string desc = R"delim({"originDescription":null,"inputTypes":["BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT"],"grouping":[0],"distinctInfos":[],
"aggInfoList":
	{
	"aggregateCalls":[{"name":"AVG($1)","aggregationFunction":"LongAvgAggFunction","argIndexes":[1],"consumeRetraction":"true","filterArg":-1}],
	"accTypes":["BIGINT","BIGINT","BIGINT"],
	"aggValueTypes":["BIGINT"],
	"indexOfCountStar":2
	}
})delim";
    json config = json::parse(desc);

    GroupAggFunction *groupAgg = new GroupAggFunction(1L, config);
    KeyedProcessOperator<RowData *, RowData *, RowData *> keyedProcessOperator(groupAgg, new OutputTest(), config);
}

TEST(KeyedProcessOperatorTest, Open)
{

    std::string desc = R"delim({"originDescription":null,"inputTypes":["BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT"],"grouping":[0],"distinctInfos":[],
"aggInfoList":
	{
	"aggregateCalls":[{"name":"AVG($1)","aggregationFunction":"LongAvgAggFunction","argIndexes":[1],"consumeRetraction":"true","filterArg":-1}],
	"accTypes":["BIGINT","BIGINT","BIGINT"],
	"aggValueTypes":["BIGINT"],
	"indexOfCountStar":2
	}
})delim";
    json config = json::parse(desc);
    GroupAggFunction *groupAgg = new GroupAggFunction(1L, config);
    KeyedProcessOperator<RowData *, RowData *, RowData *> keyedProcessOperator(groupAgg, new OutputTest(), config);
    keyedProcessOperator.setup();
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("KeyedProcessOperatorTest", 2, 1, 0)));
    std::vector<RowField> *typeInfo = new std::vector<RowField>({RowField("col1", BasicLogicalType::BIGINT), RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer *ser = new RowDataSerializer(new RowType(false, *typeInfo));

    ASSERT_NO_THROW(keyedProcessOperator.initializeState(initializer, ser));

    ASSERT_NO_THROW(keyedProcessOperator.open());
}

TEST(KeyedProcessOperatorTest, ProcessElementWithMockedUserFunction)
{

    MockUserFunction *userFunction = new MockUserFunction();

    std::string desc = R"delim({"originDescription":null,"inputTypes":["BIGINT","BIGINT"],"outputTypes":["BIGINT","BIGINT"],"grouping":[0],"distinctInfos":[],
"aggInfoList":
	{
	"aggregateCalls":[{"name":"AVG($1)","aggregationFunction":"LongAvgAggFunction","argIndexes":[1],"consumeRetraction":"true","filterArg":-1}],
	"accTypes":["BIGINT","BIGINT","BIGINT"],
	"aggValueTypes":["BIGINT"],
	"indexOfCountStar":2
	}
})delim";
    json config = json::parse(desc);

    KeyedProcessOperator<RowData *, RowData *, RowData *> keyedProcessOperator(userFunction, new OutputTest(), config);
    keyedProcessOperator.setup();
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("KeyedProcessOperatorTest", 2, 1, 0)));
    std::vector<RowField> *typeInfo = new std::vector<RowField>({RowField("col1", BasicLogicalType::BIGINT), RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer *ser = new RowDataSerializer(new RowType(false, *typeInfo));

    keyedProcessOperator.initializeState(initializer, ser);
    keyedProcessOperator.open();

    BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(2);
    row->setInt(0, 1);
    row->setInt(1, 1);
    StreamRecord *record = new StreamRecord(reinterpret_cast<void *>(row));
    keyedProcessOperator.setCurrentKey(row);
    ASSERT_NO_THROW(keyedProcessOperator.processElement(record));
    ASSERT_EQ(userFunction->isCalled, true);

    delete record;
}

TEST(KeyedProcessOperatorTest, FastTop1FunctionTest)
{
    std::string desc = R"delim({
    "originDescription": null,
    "inputTypes": [
        "BIGINT",
        "BIGINT",
        "BIGINT"
    ],
    "outputTypes": [
        "BIGINT",
        "BIGINT",
        "BIGINT"
    ],
    "partitionKey": [
        0
    ],
    "outputRankNumber": false,
    "rankRange": "rankStart=1, rankEnd=1",
    "generateUpdateBefore": false,
    "processFunction": "FastTop1Function",
    "sortFieldIndices": [
        1,
        2
    ],
    "sortAscendingOrders": [
        false,
        true
    ],
    "sortNullsIsLast": [
        true,
        false
    ]
})delim";
    json config = json::parse(desc);

    FastTop1Function<long> *fastTop1Function = new FastTop1Function<long>(config);
    BatchOutputTest* output = new BatchOutputTest();
    KeyedProcessOperator keyedProcessOperator(fastTop1Function, output, config);
    keyedProcessOperator.setup();
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("KeyedProcessOperatorTest", 2, 1, 0)));
    std::vector<RowField> *typeInfo = new std::vector<RowField>({RowField("col1", BasicLogicalType::BIGINT), RowField("col2", BasicLogicalType::BIGINT),RowField("col3", BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE)});
    TypeSerializer *ser = new RowDataSerializer(new RowType(false, *typeInfo));

    keyedProcessOperator.initializeState(initializer, ser);
    keyedProcessOperator.open();

    // test case
    /*
     * 2,5,6
     * 2,5,7
     * 3,144,14
     * 3,4,45
     * 4,4,4
     * 5,6,9
     */
    int rowCnt = 6;
    std::vector<long> col0 = {2, 2, 3, 3, 4, 5};
    std::vector<long> col1 = {5, 5, 144, 4, 4, 6};
    std::vector<long> col2 = {6, 7, 14, 45, 4, 9};
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    std::cout<<"input vectorbatch created"<<std::endl;
    StreamRecord *record = new StreamRecord(vb);
    keyedProcessOperator.processBatch(record);
    auto outputvb = output->getVectorBatch();

    /*
    * 2,5,6
    * 3,144,14
    * 4,4,4
    * 5,6,9
    */
    int rowCnt2 = 4;
    std::vector<long> expectedcol0{2, 3, 4, 5};
    std::vector<long> expectedcol1{5, 144, 4, 6};
    std::vector<long> expectedcol2{6, 14, 4, 9};
    omnistream::VectorBatch* expectedvb = new omnistream::VectorBatch(rowCnt2);
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol0.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol1.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol2.data()));
    bool matched = omniruntime::TestUtil::VecBatchMatch(outputvb, expectedvb);
    EXPECT_EQ(matched, true);
}

TEST(KeyedProcessOperatorTest, Appened)
{
    std::string desc = R"delim({
    "originDescription": null,
    "inputTypes": [
        "BIGINT",
        "BIGINT",
        "BIGINT"
    ],
    "outputTypes": [
        "BIGINT",
        "BIGINT",
        "BIGINT"
    ],
    "partitionKey": [
        0
    ],
    "outputRankNumber": true,
    "rankRange": "rankStart=1, rankEnd=3",
    "generateUpdateBefore": false,
    "processFunction": "AppendOnlyTopNFunction",
    "sortFieldIndices": [
        1
    ],"sortAscendingOrders": [
        false
    ],"sortNullsIsLast": [
        true
    ]})delim";
    json config = json::parse(desc);

    AppendOnlyTopNFunction<long> *TopNFunction = new AppendOnlyTopNFunction<long>(config);
    BatchOutputTest* output = new BatchOutputTest();
    KeyedProcessOperator keyedProcessOperator(TopNFunction, output, config);
    keyedProcessOperator.setup();
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("KeyedProcessOperatorTest", 2, 1, 0)));
    std::vector<RowField> *typeInfo = new std::vector<RowField>({RowField("col1", BasicLogicalType::BIGINT), RowField("col2", BasicLogicalType::BIGINT),RowField("col3", BasicLogicalType::BIGINT)});
    TypeSerializer *ser = new RowDataSerializer(new RowType(false, *typeInfo));

    keyedProcessOperator.initializeState(initializer, ser);
    keyedProcessOperator.open();

    // test case
    /*
     * 2,5,6
     * 2,5,7
     * 3,144,14
     * 3,4,45
     * 4,4,4
     * 5,6,9
     */
    int rowCnt = 6;
    std::vector<long> col0 = {1, 1, 1, 1, 1, 5};
    std::vector<long> col1 = {5, 5, 144, 14, 24, 6};
    std::vector<long> col2 = {6, 7, 14, 45, 4, 9};
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    std::cout<<"input vectorbatch created"<<std::endl;
    StreamRecord *record = new StreamRecord(vb);
    keyedProcessOperator.processBatch(record);
    auto outputvb = output->getVectorBatch();

    /*
    * 2,5,6
    * 3,144,14
    * 4,4,4
    * 5,6,9
    */
    int rowCnt2 = 10;
    std::vector<long> expectedcol0 = {1, 1, 1, 1, 1, 1, 1, 1, 1, 5};
    std::vector<long> expectedcol1 = {5, 5, 144, 5, 5, 14, 5, 24, 14, 6};
    std::vector<long> expectedcol2 = {6, 7, 14, 6, 7, 45, 6, 4, 45, 9};
    std::vector<long> expectedcol3 = {1, 2, 1, 2, 3, 2, 3, 2, 3, 1};
    omnistream::VectorBatch* expectedvb = new omnistream::VectorBatch(rowCnt2);
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol0.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol1.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol2.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol3.data()));
    bool matched = omniruntime::TestUtil::VecBatchMatch(outputvb, expectedvb);
    EXPECT_EQ(matched, true);
}


TEST(KeyedProcessOperatorTest, DISABLED_Appened2)
{
    std::string desc = R"delim({
    "originDescription": null,
    "inputTypes": [
        "BIGINT",
        "BIGINT",
        "BIGINT"
    ],
    "outputTypes": [
        "BIGINT",
        "BIGINT",
        "BIGINT"
    ],
    "partitionKey": [
        0
    ],
    "outputRankNumber": true,
    "rankRange": "rankStart=1, rankEnd=3",
    "generateUpdateBefore": false,
    "processFunction": "AppendOnlyTopNFunction",
    "sortFieldIndices": [
        1, 2
    ],"sortAscendingOrders": [
        false, true
    ],"sortNullsIsLast": [
        true, false
    ]})delim";
    json config = json::parse(desc);

    AppendOnlyTopNFunction<long> *TopNFunction = new AppendOnlyTopNFunction<long>(config);
    BatchOutputTest* output = new BatchOutputTest();
    KeyedProcessOperator keyedProcessOperator(TopNFunction, output, config);
    keyedProcessOperator.setup();
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("KeyedProcessOperatorTest", 2, 1, 0)));
    std::vector<RowField> *typeInfo = new std::vector<RowField>({RowField("col1", BasicLogicalType::BIGINT), RowField("col2", BasicLogicalType::BIGINT),RowField("col3", BasicLogicalType::BIGINT)});
    TypeSerializer *ser = new RowDataSerializer(new RowType(false, *typeInfo));

    keyedProcessOperator.initializeState(initializer, ser);
    keyedProcessOperator.open();

    // test case
    /*
     * 2,5,6
     * 2,5,7
     * 3,144,14
     * 3,4,45
     * 4,4,4
     * 5,6,9
     */
    int rowCnt = 6;
    std::vector<long> col0 = {1, 1, 1, 1, 1, 5};
    std::vector<long> col1 = {5, 5, 144, 14, 24, 6};
    std::vector<long> col2 = {6, 7, 14, 45, 4, 9};
    omnistream::VectorBatch* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col0.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col1.data()));
    vb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt, col2.data()));
    std::cout<<"input vectorbatch created"<<std::endl;
    StreamRecord *record = new StreamRecord(vb);
    keyedProcessOperator.processBatch(record);
    auto outputvb = output->getVectorBatch();

    /*
    * 2,5,6
    * 3,144,14
    * 4,4,4
    * 5,6,9
    */
    int rowCnt2 = 10;
    std::vector<long> expectedcol0 = {1, 1, 1, 1, 1, 1, 1, 1, 1, 5};
    std::vector<long> expectedcol1 = {5, 5, 144, 5, 5, 14, 5, 24, 14, 6};
    std::vector<long> expectedcol2 = {6, 7, 14, 6, 7, 45, 6, 4, 45, 9};
    std::vector<long> expectedcol3 = {1, 2, 1, 2, 3, 2, 3, 2, 3, 1};
    omnistream::VectorBatch* expectedvb = new omnistream::VectorBatch(rowCnt2);
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol0.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol1.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol2.data()));
    expectedvb->Append(omniruntime::TestUtil::CreateVector<int64_t>(rowCnt2, expectedcol3.data()));
    bool matched = omniruntime::TestUtil::VecBatchMatch(outputvb, expectedvb);
    EXPECT_EQ(matched, true);
}