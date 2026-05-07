#include "streaming/api/operators/StreamOperatorStateHandler.h"
#include "streaming/api/operators/StreamTaskStateInitializerImpl.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/execution/Environment.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"

#include <gtest/gtest.h>

TEST(StreamOperatorStateHandlerTest, Init)
{
    omnistream::EnvironmentV2* env = new omnistream::RuntimeEnvironmentV2();
    StreamTaskStateInitializerImpl initializer(env);
    StreamOperatorStateContextImpl<int> *context = initializer.streamOperatorStateContext<int>(new IntSerializer(), nullptr, nullptr);
    ASSERT_NO_THROW(context->keyedStateBackend());
}