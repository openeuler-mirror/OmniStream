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

#ifndef MAILBOXEXECUTORIMPL_H
#define MAILBOXEXECUTORIMPL_H

#include "MailboxExecutor.h"

#include <utility>
#include <vector>
#include <memory>
#include <string>
#include <stdexcept>
#include <optional>

#include "MailboxProcessor.h"

namespace omnistream {
    class MailboxExecutorImpl : public MailboxExecutor {
    public:
        MailboxExecutorImpl(std::shared_ptr<TaskMailbox> mailbox, int priority,
                            std::shared_ptr<StreamTaskActionExecutor> actionExecutor)
            : mailbox(std::move(mailbox)),
              priority(priority),
              actionExecutor(std::move(actionExecutor))
        {
        }

        MailboxExecutorImpl(std::shared_ptr<TaskMailbox> mailbox, int priority,
                            std::shared_ptr<StreamTaskActionExecutor> actionExecutor,
                            std::shared_ptr<MailboxProcessor> mailboxProcessor)
            : mailbox(std::move(mailbox)),
              priority(priority),
              actionExecutor(std::move(actionExecutor)),
              mailboxProcessor_(std::move(mailboxProcessor))
        {
        }

        void execute(std::shared_ptr<ThrowingRunnable> command, const std::string &description) override
        {
                auto mail = std::make_shared<Mail>(std::move(command),
                    priority,
                    actionExecutor,
                    description,
                    std::vector<std::string>());
                mailbox->put(mail);
        }

        void execute(std::shared_ptr<ThrowingRunnable> command, const std::string &descriptionFormat,
                     const std::vector<std::any> &descriptionArgs) override
        {
            auto mail = std::make_shared<Mail>(std::move(command),
                    priority,
                    actionExecutor,
                    descriptionFormat,
                    std::vector<std::string>());
            mailbox->put(mail);
        }

        void yield() override
        {
            auto mail = mailbox->take(priority);

            mail->run();
        }

        bool tryYield() override
        {
            auto optionalMail = mailbox->tryTake(priority);
            if (optionalMail.has_value()) {
                (*optionalMail)->run();

                return true;
            } else {
                return false;
            }
        }

        std::string toString() const override
        {
            return "MailboxExecutorImpl";
        }

    private:
        const std::shared_ptr<TaskMailbox> mailbox;
        const int priority;
        const std::shared_ptr<StreamTaskActionExecutor> actionExecutor;
        std::shared_ptr<MailboxProcessor> mailboxProcessor_;
    };
} // namespace omnistream

#endif // MAILBOXEXECUTORIMPL_H