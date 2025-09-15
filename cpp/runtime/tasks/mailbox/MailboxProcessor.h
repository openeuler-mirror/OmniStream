/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef MAILBOX_PROCESSOR_H
#define MAILBOX_PROCESSOR_H

#include <memory>
#include <vector>
#include <optional>
#include <string>
#include <sstream>
#include "TaskMailbox.h"
#include "MailboxDefaultAction.h"
#include "StreamTaskActionExecutor.h"
#include "Mail.h"
#include "MailboxExecutor.h"
#include "PeriodTimer.h"


namespace omnistream
{
    class MailboxProcessor :public std::enable_shared_from_this<MailboxProcessor>
    {
    public:
        MailboxProcessor();
        explicit MailboxProcessor(std::shared_ptr<MailboxDefaultAction> mailboxDefaultAction);
        MailboxProcessor(std::shared_ptr<MailboxDefaultAction> mailboxDefaultAction,
                         std::shared_ptr<StreamTaskActionExecutor> actionExecutor);
        MailboxProcessor(std::shared_ptr<MailboxDefaultAction> mailboxDefaultAction,
                         std::shared_ptr<TaskMailbox> mailbox,
                         std::shared_ptr<StreamTaskActionExecutor> actionExecutor);
        ~MailboxProcessor();

        std::shared_ptr<MailboxExecutor> getMainMailboxExecutor();
        std::shared_ptr<MailboxExecutor> getMailboxExecutor(int priority);

        void prepareClose();
        void close();
        void drain() ;
        void runMailboxLoop() ;
        void suspend();
        bool runMailboxStep() ;
        bool isMailboxThread();
        void reportThrowable(std::exception_ptr throwable);
        void allActionsCompleted();

        bool isDefaultActionAvailable();
        bool isMailboxLoopRunning();
        bool hasMail();

        std::string toString() const;

        void sendPoisonMail(std::shared_ptr<ThrowingRunnable> mail);
        void sendPoisonMail(std::shared_ptr<ThrowingRunnable> mail,  const std::string& descriptionFormat);
        void sendControlMail(std::shared_ptr<ThrowingRunnable> mail, const std::string& descriptionFormat,
                             const std::vector<std::string>& descriptionArgs = {});
        bool processMail(std::shared_ptr<TaskMailbox> mailbox, bool singleStep) ;
        bool processMailsWhenDefaultActionUnavailable() ;
        bool processMailsNonBlocking(bool singleStep) ;
        void maybePauseIdleTimer();
        void maybeRestartIdleTimer();
        std::shared_ptr<MailboxDefaultAction::Suspension> suspendDefaultAction(
            std::shared_ptr<PeriodTimer> suspensionTimer);
        bool isNextLoopPossible();

        class DefaultActionSuspension : public MailboxDefaultAction::Suspension {
        public:
            explicit DefaultActionSuspension(std::shared_ptr<PeriodTimer> suspensionTimer) : suspensionTimer(
                suspensionTimer) {
            };
            ~DefaultActionSuspension() override;
            void resume() override;
            std::shared_ptr<PeriodTimer> getSuspensionTimer() {
                return suspensionTimer;
            };

        private:
            void resumeInternal() {};
            std::shared_ptr<PeriodTimer> suspensionTimer;
        };

        class lambdaHelper
        {
           public:
            explicit lambdaHelper(std::shared_ptr<MailboxProcessor> mailbox_processor) :processor_(mailbox_processor){};
            std::shared_ptr<MailboxProcessor> processor_;
            void suspended(bool suspended)
            {
                processor_->suspended = suspended;
            }

            void mailboxLoopRunning(bool mailboxLoopRunning)
            {
                processor_->mailboxLoopRunning = mailboxLoopRunning;
            }

            std::shared_ptr<TaskMailbox> getMailbox()
            {
                return processor_->mailbox_;
            }

            std::shared_ptr<MailboxProcessor> getOwner()
            {
                return processor_;
            }
        };
    private:
        // inner class forward declaration
        std::shared_ptr<TaskMailbox> mailbox_;
        std::shared_ptr<MailboxDefaultAction> mailboxDefaultAction;
        bool mailboxLoopRunning;
        bool suspended;
        std::shared_ptr<DefaultActionSuspension> suspendedDefaultAction;
        std::shared_ptr<StreamTaskActionExecutor> actionExecutor;

        int workaround =0;
    };


} // namespace omnistream

#endif // MAILBOX_PROCESSOR_H
