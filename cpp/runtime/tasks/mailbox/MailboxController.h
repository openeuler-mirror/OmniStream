/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/24/25.
//

// MailboxController.h
#ifndef MAILBOXCONTROLLER_H
#define MAILBOXCONTROLLER_H

#include <memory>
#include <string>

#include "MailboxDefaultAction.h"

#include "MailboxProcessor.h"

namespace omnistream {

    class MailboxController : public MailboxDefaultAction::Controller {
    public:
        explicit MailboxController(std::shared_ptr<MailboxProcessor> mailboxProcessor);
        ~MailboxController() override;

        void allActionsCompleted() override;
        std::shared_ptr<MailboxDefaultAction::Suspension> suspendDefaultAction(std::shared_ptr<PeriodTimer> suspensionPeriodTimer) override;
        std::shared_ptr<MailboxDefaultAction::Suspension> suspendDefaultAction() override;

        std::string toString() const;

    private:
        std::shared_ptr<MailboxProcessor> mailboxProcessor_;
    };

} // namespace omnistream

#endif // MAILBOXCONTROLLER_H