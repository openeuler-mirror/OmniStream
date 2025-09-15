/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "MailboxController.h"
#include "MailboxProcessor.h"
#include "PeriodTimer.h"
#include "MailboxDefaultAction.h"
#include <sstream>

namespace omnistream {

    MailboxController::MailboxController(std::shared_ptr<MailboxProcessor> mailboxProcessor)
        : mailboxProcessor_(mailboxProcessor) {}

    MailboxController::~MailboxController() {}

    void MailboxController::allActionsCompleted() {
        mailboxProcessor_->allActionsCompleted();
    }

    std::shared_ptr<MailboxDefaultAction::Suspension> MailboxController::suspendDefaultAction(std::shared_ptr<PeriodTimer> suspensionPeriodTimer) {
        return mailboxProcessor_->suspendDefaultAction(suspensionPeriodTimer);
    }

    std::shared_ptr<MailboxDefaultAction::Suspension> MailboxController::suspendDefaultAction() {
        return mailboxProcessor_->suspendDefaultAction(nullptr);
    }

    std::string MailboxController::toString() const {
        std::stringstream ss;
        ss << "MailboxController[mailboxProcessor=" << (mailboxProcessor_ ? "present" : "nullptr") << "]";
        return ss.str();
    }

} // namespace omnistream