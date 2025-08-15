/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/22/25.
//
// MailboxDefaultAction.h
#ifndef MAILBOX_DEFAULT_ACTION_H
#define MAILBOX_DEFAULT_ACTION_H

#include <memory>

#include "PeriodTimer.h"

namespace omnistream {

    class MailboxDefaultAction {
    public:
        class Suspension {
        public:
            virtual void resume() = 0;
            virtual ~Suspension() = default;
        };

        class Controller {
        public:
            virtual void allActionsCompleted() = 0;
            virtual std::shared_ptr<Suspension> suspendDefaultAction() = 0;
            virtual std::shared_ptr<Suspension> suspendDefaultAction(std::shared_ptr<PeriodTimer> periodTimer) = 0;
            virtual ~Controller() = default;
        };

        virtual void runDefaultAction(std::shared_ptr<Controller> controller) = 0;
        virtual ~MailboxDefaultAction() = default;
    };

} // namespace omnistream

#endif // MAILBOX_DEFAULT_ACTION_H
