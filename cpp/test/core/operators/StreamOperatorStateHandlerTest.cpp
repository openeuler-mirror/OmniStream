#include "core/operators/StreamOperatorStateHandler.h"
#include "core/operators/StreamTaskStateInitializerImpl.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/execution/Environment.h"
#include "runtime/taskmanager/RuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"

#include <gtest/gtest.h>

TEST(StreamOperatorStateHandlerTest, Init)
{
    Environment *env = new RuntimeEnvironment(new TaskInfoImpl("Test", 2, 1, 0));
    StreamTaskStateInitializerImpl initializer(env);
    StreamOperatorStateContextImpl<int> *context = initializer.streamOperatorStateContext<int>(new IntSerializer(), nullptr, nullptr);
    ASSERT_NO_THROW(context->keyedStateBackend());
}