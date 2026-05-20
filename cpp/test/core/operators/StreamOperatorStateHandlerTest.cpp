#include "streaming/api/operators/StreamOperatorStateHandler.h"
#include "streaming/api/operators/StreamTaskStateInitializerImpl.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/execution/Environment.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include "runtime/state/TaskStateManager.h"

#include <gtest/gtest.h>

TEST(StreamOperatorStateHandlerTest, Init)
{
    auto env = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl initializer(env);
    StreamOperatorStateContextImpl<int> *context = initializer.streamOperatorStateContext<int>(new IntSerializer(), nullptr, nullptr);
    ASSERT_NO_THROW(context->keyedStateBackend());
    delete context;
    delete taskInfo;
    delete env;
}