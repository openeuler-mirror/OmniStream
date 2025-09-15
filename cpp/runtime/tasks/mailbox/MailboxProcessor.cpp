/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "MailboxProcessor.h"

#include <stdexcept>
#include <sstream>
#include <algorithm>
#include <climits>

#include "MailboxController.h"
#include "MailboxExecutorImpl.h"
#include "TaskMailboxImpl.h"

namespace omnistream {
MailboxProcessor::MailboxProcessor()
{}

MailboxProcessor::MailboxProcessor(std::shared_ptr<MailboxDefaultAction> mailboxDefaultAction)
    : MailboxProcessor(mailboxDefaultAction, StreamTaskActionExecutor::IMMEDIATE)
{}

MailboxProcessor::MailboxProcessor(std::shared_ptr<MailboxDefaultAction> mailboxDefaultAction,
    std::shared_ptr<StreamTaskActionExecutor> actionExecutor)
    : MailboxProcessor(
          mailboxDefaultAction, std::make_shared<TaskMailboxImpl>(std::this_thread::get_id()), actionExecutor)
{}

MailboxProcessor::MailboxProcessor(std::shared_ptr<MailboxDefaultAction> mailboxDefaultAction,
    std::shared_ptr<TaskMailbox> mailbox, std::shared_ptr<StreamTaskActionExecutor> actionExecutor)
    : mailbox_(mailbox), mailboxDefaultAction(mailboxDefaultAction), mailboxLoopRunning(true), suspended(false),
      suspendedDefaultAction(nullptr), actionExecutor(actionExecutor)
{}

MailboxProcessor::~MailboxProcessor()
{}

std::shared_ptr<MailboxExecutor> MailboxProcessor::getMainMailboxExecutor()
{
    return std::make_shared<MailboxExecutorImpl>(mailbox_, TaskMailbox::MIN_PRIORITY, actionExecutor);
}

std::shared_ptr<MailboxExecutor> MailboxProcessor::getMailboxExecutor(int priority)
{
    return std::make_shared<MailboxExecutorImpl>(mailbox_, priority, actionExecutor, shared_from_this());
}

void MailboxProcessor::prepareClose()
{
    mailbox_->quiesce();
}

void MailboxProcessor::close()
{
    auto droppedMails = mailbox_->close();
    if (!droppedMails.empty()) {
        // Log dropped mails (if logging is available)
        std::exception_ptr maybeErr;
        for (auto &droppedMail : droppedMails) {
            try {
                droppedMail->tryCancel();
            } catch (...) {
                if (!maybeErr) {
                    maybeErr = std::current_exception();
                }
            }
        }
        if (maybeErr) {
            std::rethrow_exception(maybeErr);
        }
    }
}

void MailboxProcessor::drain()
{
    for (auto &mail : mailbox_->drain()) {
        mail->run();
    }
}

void MailboxProcessor::runMailboxLoop()
{
    LOG("runMailboxLoop  start")
    suspended = !mailboxLoopRunning;

    LOG("mailbox localmailbox")
    auto localMailbox = mailbox_;

    LOG(">>>localMailbox  isMailboxThread")
    if (!localMailbox->isMailboxThread()) {
        throw std::runtime_error("Method must be executed by declared mailbox thread!");
    }

    if (localMailbox->getState() != TaskMailbox::State::OPEN) {
        throw std::runtime_error("Mailbox must be opened!");
    }
    LOG(">>>before isNextLoopPossible")
    while (isNextLoopPossible()) {
        // LOG(">>>before procsiyessMail")
        // workaround
        workaround++;
        // LOG_TRACE( "Round number " << workaround)
        // LOG("The current mailbox_ before processMail :" << mailbox_->toString())

        processMail(mailbox_, false);
        if (isNextLoopPossible()) {
            // LOG(">>>before runDefaultAction")
            mailboxDefaultAction->runDefaultAction(std::make_shared<MailboxController>(shared_from_this()));
        }
    }
    LOG(">>>after isNextLoopPossible")
}

class LambdaSuspend : public MailboxProcessor::lambdaHelper {
public:
    explicit LambdaSuspend(std::shared_ptr<MailboxProcessor> obj1) : lambdaHelper(obj1){};

    void lambda()
    {
        lambdaHelper::suspended(true);
    }
};
void MailboxProcessor::suspend()
{
    auto runnable = std::make_shared<MemberFunctionRunnable<LambdaSuspend>>(
        std::make_shared<LambdaSuspend>(shared_from_this()), &LambdaSuspend::lambda, "Suspend");
    sendPoisonMail(runnable);
}

bool MailboxProcessor::runMailboxStep()
{
    suspended = !mailboxLoopRunning;

    if (processMail(mailbox_, true)) {
        return true;
    }
    if (isDefaultActionAvailable() && isNextLoopPossible()) {
        mailboxDefaultAction->runDefaultAction(std::make_shared<MailboxController>(shared_from_this()));
        return true;
    }
    return false;
}

bool MailboxProcessor::isMailboxThread()
{
    return mailbox_->isMailboxThread();
}

class LambdaReportThrowable {
public:
    explicit LambdaReportThrowable(std::exception_ptr throwable) : throwable_(throwable){};
    std::exception_ptr throwable_;
    void lambda()
    {
        std::rethrow_exception(throwable_);
    }
};

void MailboxProcessor::reportThrowable(std::exception_ptr throwable)
{
    auto runnable = std::make_shared<MemberFunctionRunnable<LambdaReportThrowable>>(
        std::make_shared<LambdaReportThrowable>(throwable), &LambdaReportThrowable::lambda);
    sendControlMail(runnable, "Report throwable");
}

class LambdaAllActionCompleted : public MailboxProcessor::lambdaHelper {
public:
    explicit LambdaAllActionCompleted(std::shared_ptr<MailboxProcessor> obj1) : lambdaHelper(obj1){};
    void lambda()
    {
        lambdaHelper::suspended(true);
        lambdaHelper::mailboxLoopRunning(false);
    }
};

void MailboxProcessor::allActionsCompleted()
{
    auto runnable = std::make_shared<MemberFunctionRunnable<LambdaAllActionCompleted>>(
        std::make_shared<LambdaAllActionCompleted>(shared_from_this()), &LambdaAllActionCompleted::lambda);
    sendPoisonMail(runnable);
}

bool MailboxProcessor::isDefaultActionAvailable()
{
    return suspendedDefaultAction == nullptr;
}

bool MailboxProcessor::isMailboxLoopRunning()
{
    return mailboxLoopRunning;
}

bool MailboxProcessor::hasMail()
{
    return mailbox_->hasMail();
}

std::string MailboxProcessor::toString() const
{
    std::stringstream ss;
    ss << "MailboxProcessor";
    return ss.str();
}

class LambdaSendPoisonMail : public MailboxProcessor::lambdaHelper {
public:
    LambdaSendPoisonMail(std::shared_ptr<MailboxProcessor> processor, std::shared_ptr<ThrowingRunnable> runnableMail)
        : lambdaHelper(processor), runnableMail_(runnableMail){};
    void lambda()
    {
        auto mailbox = lambdaHelper::getMailbox();
        if (mailbox->getState() == TaskMailbox::State::OPEN) {
            std::vector<std::string> stringVector;
            processor_->sendControlMail(runnableMail_, "PoisonMail" + runnableMail_ ->toString(), stringVector);
        }
    }
    std::shared_ptr<ThrowingRunnable> runnableMail_;
};

void MailboxProcessor::sendPoisonMail(std::shared_ptr<ThrowingRunnable> runnableMail)
{
    auto runnable = std::make_shared<MemberFunctionRunnable<LambdaSendPoisonMail>>(
        std::make_shared<LambdaSendPoisonMail>(shared_from_this(), runnableMail), &LambdaSendPoisonMail::lambda);
    mailbox_->runExclusively(runnable);
}

void MailboxProcessor::sendControlMail(std::shared_ptr<ThrowingRunnable> runnable, const std::string &descriptionFormat,
    const std::vector<std::string> &descriptionArgs)
{
    auto mail = std::make_shared<Mail>(runnable, INT_MAX, descriptionFormat, descriptionArgs);
    mailbox_->putFirst(mail);
    LOG("MailboxProcessor::sendControlMail after putFirst " << mailbox_->toString());
}

bool MailboxProcessor::processMail(std::shared_ptr<TaskMailbox> mailbox, bool singleStep)
{
    bool isBatchAvailable = mailbox->createBatch();
    bool processed = isBatchAvailable && processMailsNonBlocking(singleStep);
    if (singleStep) {
        return processed;
    }
    processed |= processMailsWhenDefaultActionUnavailable();
    return processed;
}

bool MailboxProcessor::processMailsWhenDefaultActionUnavailable()
{
    bool processedSomething = false;
    std::optional<std::shared_ptr<Mail>> maybeMail;
    while (!isDefaultActionAvailable() && isNextLoopPossible()) {
        maybeMail = mailbox_->tryTake(TaskMailbox::MIN_PRIORITY);
        if (!maybeMail) {
            maybeMail = mailbox_->take(TaskMailbox::MIN_PRIORITY);
        }
        maybePauseIdleTimer();
        maybeMail.value()->run();
        maybeRestartIdleTimer();
        processedSomething = true;
    }
    return processedSomething;
}

bool MailboxProcessor::processMailsNonBlocking(bool singleStep)
{
    long processedMails = 0;
    std::optional<std::shared_ptr<Mail>> maybeMail;

    while (isNextLoopPossible() && (maybeMail = mailbox_->tryTakeFromBatch())) {
        if (processedMails++ == 0) {
            maybePauseIdleTimer();
        }
        maybeMail.value()->run();
        if (singleStep) {
            break;
        }
    }
    if (processedMails > 0) {
        maybeRestartIdleTimer();
        return true;
    } else {
        return false;
    }
}

void MailboxProcessor::maybePauseIdleTimer()
{
    if (suspendedDefaultAction && suspendedDefaultAction->getSuspensionTimer()) {
        suspendedDefaultAction->getSuspensionTimer()->markEnd();
    }
}

void MailboxProcessor::maybeRestartIdleTimer()
{
    if (suspendedDefaultAction && suspendedDefaultAction->getSuspensionTimer()) {
        suspendedDefaultAction->getSuspensionTimer()->markStart();
    }
}

std::shared_ptr<MailboxDefaultAction::Suspension> MailboxProcessor::suspendDefaultAction(
    std::shared_ptr<PeriodTimer> suspensionTimer)
{
    if (!mailbox_->isMailboxThread()) {
        throw std::runtime_error("Suspending must only be called from the mailbox thread!");
    }

    if (suspendedDefaultAction) {
        throw std::runtime_error("Default action has already been suspended");
    }
    if (!suspendedDefaultAction) {
        suspendedDefaultAction = std::make_shared<DefaultActionSuspension>(suspensionTimer);
    }

    maybeRestartIdleTimer();
    return suspendedDefaultAction;
}

bool MailboxProcessor::isNextLoopPossible()
{
    return !suspended;
}

void MailboxProcessor::sendPoisonMail(std::shared_ptr<ThrowingRunnable> mail, const std::string &descriptionFormat)
{
    auto runnable = std::make_shared<MemberFunctionRunnable<LambdaSendPoisonMail>>(
        std::make_shared<LambdaSendPoisonMail>(shared_from_this(), mail), &LambdaSendPoisonMail::lambda);
    mailbox_->runExclusively(runnable);
}

MailboxProcessor::DefaultActionSuspension::~DefaultActionSuspension() {
}

void MailboxProcessor::DefaultActionSuspension::resume() {
}

}  // namespace omnistream

// MailboxProcessor::MailboxController::MailboxController(MailboxProcessor* mailboxProcessor) :
// mailboxProcessor(mailboxProcessor
