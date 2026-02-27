/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef MAILBOXCONTROLLER_H
#define MAILBOXCONTROLLER_H

#include <memory>
#include <string>

#include "MailboxDefaultAction.h"

#include "MailboxProcessor.h"

namespace omnistream {

    class MailboxController : public MailboxDefaultAction::Controller {
    public:
        explicit MailboxController(MailboxProcessor* mailboxProcessor);

        ~MailboxController() override;

        void allActionsCompleted() override;
        MailboxDefaultAction::Suspension* suspendDefaultAction(std::shared_ptr<PeriodTimer> suspensionPeriodTimer) override;
        MailboxDefaultAction::Suspension* suspendDefaultAction() override;

        std::string toString() const;

    private:
        MailboxProcessor* mailboxProcessor_;
    };

} // namespace omnistream

#endif // MAILBOXCONTROLLER_H