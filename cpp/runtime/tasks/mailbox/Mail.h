/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef MAIL_H
#define MAIL_H

#include <string>
#include <memory>
#include <vector>
#include <sstream>
#include <iostream>

#include "StreamTaskActionExecutor.h"

namespace omnistream
{
    class Mail
    {
    public:
        Mail();

        Mail(std::shared_ptr<ThrowingRunnable> runnable,
            int priority,
            std::shared_ptr<StreamTaskActionExecutor> actionExecutor,
             const std::string& descriptionFormat,
             const std::vector<std::string>& descriptionArgs);

        Mail(std::shared_ptr<ThrowingRunnable> runnable, int priority, const std::string& descriptionFormat,
             const std::vector<std::string>& descriptionArgs);

        ~Mail();

        int getPriority() const;
        void tryCancel();
        std::string toString() const;
        void run();

    private:
        std::shared_ptr<ThrowingRunnable> runnable_;
        int priority_;
        std::string descriptionFormat_;
        std::vector<std::string> descriptionArgs_;
        std::shared_ptr<StreamTaskActionExecutor> actionExecutor_;
    };
} // namespace omnistream

#endif // MAIL_H
