/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAMTASK_H
#define OMNISTREAMTASK_H

#include <KeySelector.h>
#include <string>
#include <partitioner/StreamPartitionerV2.h>
#include <taskmanager/OmniRuntimeEnvironment.h>
#include <runtime/io/writer/RecordWriterDelegateV2.h>
#include <tasks/OperatorChain.h>
#include <partitioner/KeyGroupStreamPartitionerV2.h>
#include <utils/monitormmap/SHMMetric.h>

#include "mailbox/MailboxProcessor.h"
#include "mailbox/StreamTaskActionExecutor.h"
#include "mailbox/TaskMailbox.h"
#include "io/OmniStreamInputProcessor.h"
#include "io/network/api/StopMode.h"


namespace omnistream {
    class OperatorChainV2;
    class OmniStreamTask : public std::enable_shared_from_this<OmniStreamTask> {
    public:
        explicit OmniStreamTask(std::shared_ptr<RuntimeEnvironmentV2>& env);

        OmniStreamTask(std::shared_ptr<RuntimeEnvironmentV2>& env,
                       std::shared_ptr<StreamTaskActionExecutor> actionExecutor);
        OmniStreamTask(std::shared_ptr<RuntimeEnvironmentV2>& env,
                       std::shared_ptr<StreamTaskActionExecutor> actionExecutor,
                       std::shared_ptr<TaskMailbox> mailbox);

        virtual ~OmniStreamTask() = default;

        // getter
        virtual const std::string getName() const;

        [[nodiscard]] std::shared_ptr<RuntimeEnvironmentV2> env() const
        {
            return env_;
        }

        virtual void postConstruct();
        // life cycle
        // in the most cases, it does not to be overridden
        void restore();

        // in the most cases, it does not to be overridden
        void restoreInternal();

        void cancel();

        // most case to be overrided by subclass
        virtual void init()
        {
        };

        void restoreGates();

        // life cycle the main task
        virtual void invoke();

        virtual void cleanup();

        void releaseOutputResource();

        bool IsUsingNonBlockingInput();
        // stream task specific
        virtual void processInput(std::shared_ptr<MailboxDefaultAction::Controller> controller);

        void dispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString);

    protected:
        std::shared_ptr<RuntimeEnvironmentV2> env_;
        TaskInformationPOD taskConfiguration_;

        // operator chain
        omnistream::OperatorChainV2* operatorChain = nullptr;
        std::shared_ptr<RecordWriterDelegateV2> recordWriter_;
        StreamOperator* mainOperator_;
        std::shared_ptr<OmniStreamInputProcessor> inputProcessor_;

        /**
        * All actions outside of the task {@link #mailboxProcessor mailbox} (i.e. performed by another
        * thread) must be executed through this executor to ensure that we don't have concurrent method
        * calls that void consistent checkpoints. The execution will always be performed in the task
        * thread.
        *
        * <p>CheckpointLock is superseded by {@link MailboxExecutor}, with {@link
        * StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor
        * SynchronizedStreamTaskActionExecutor} to provide lock to {@link SourceStreamTask}. */
        std::shared_ptr<StreamTaskActionExecutor> actionExecutor_;

        // mailbox loop
        std::shared_ptr<TaskMailbox> mailbox_;

        std::shared_ptr<MailboxProcessor> mailboxProcessor_;
        std::shared_ptr<MailboxExecutor> mainMailboxExecutor_;

        std::string taskName_;

        SHMMetric *shmmetric_processInput {};
        long int processInputCounter {};

        long int numberOfRows {0};

        void EndData(StopMode stopMode);
        virtual void AdvanceToEndOfEventTime()
        {
            // do nothing
        };
    private:
        // record writer
        std::shared_ptr<RecordWriterDelegateV2> createRecordWriterDelegate(
            TaskInformationPOD taskConfig, std::shared_ptr<RuntimeEnvironmentV2> environment);

        std::vector<RecordWriterV2*> createRecordWriters(TaskInformationPOD taskConfig,
                                                       std::shared_ptr<RuntimeEnvironmentV2> environment);
        RecordWriterV2* createRecordWriter(
            StreamEdgePOD edge, int outputIndex,
            std::shared_ptr<RuntimeEnvironmentV2> environment,
            std::string taskName,
            long bufferTimeout);

        template<typename K> KeySelector<K>* buildKeySelector(std::vector<KeyFieldInfoPOD>& keyFields);

        // partitioner
        StreamPartitionerV2<StreamRecord >* createPartitionerFromDesc(StreamPartitionerPOD partitioner);

        // mailbox
        void runMailboxLoop();

        /** Flags indicating the finished method of all the operators are called. */
        bool finishedOperators {false};

        std::atomic<bool> endOfDataReceived {false};
    };

    class StreamTaskAction : public MailboxDefaultAction {
    public:
        explicit StreamTaskAction(std::shared_ptr<OmniStreamTask> task) : task_(task) {};
        ~StreamTaskAction() override = default;
        void runDefaultAction(std::shared_ptr<Controller> controller) override
        {
            task_->processInput(controller);
        };

    private:
        std::shared_ptr<OmniStreamTask> task_;
    };
}


#endif // OMNISTREAMTASK_H
