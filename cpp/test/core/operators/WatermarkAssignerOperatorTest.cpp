#include "core/operators/StreamOperatorStateHandler.h"
#include "core/operators/StreamTaskStateInitializerImpl.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/execution/Environment.h"
#include "runtime/taskmanager/RuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include "OutputTest.h"
#include "core/operators/WatermarkAssignerOperator.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include <gtest/gtest.h>

TEST(WatermarkAssignerOperatorTest, InitTest)
{
    int timeRowIndex = 0;
    long outOfOrderT = 4000;
    long emissionInterval = 200;
    OutputTest *out = new OutputTest();
    WatermarkAssignerOperator *op = new WatermarkAssignerOperator(out, timeRowIndex, outOfOrderT, emissionInterval);
    op->setProcessingTimeService(new SystemProcessingTimeService());
    op->open();

    int rowCount = 5;
    omnistream::VectorBatch vb1(rowCount);
    std::vector<long> vec1 {42, 142, 242, 342, 442};
    std::vector<long> vec2 {41, 42, 43, 44, 45};
    vb1.Append(omniruntime::TestUtil::CreateVector(rowCount, vec1.data()));
    vb1.Append(omniruntime::TestUtil::CreateVector(rowCount, vec2.data()));

    StreamRecord* record1 = new StreamRecord(&vb1);
    op->processBatch(record1);
    op->finish();
}