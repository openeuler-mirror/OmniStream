/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "Mail.h"

namespace omnistream {
Mail::Mail()
    : runnable_(nullptr), priority_(0), descriptionFormat_(""), descriptionArgs_({}), actionExecutor_(nullptr){};

Mail::Mail(std::shared_ptr<ThrowingRunnable> runnable, int priority,
    std::shared_ptr<StreamTaskActionExecutor> actionExecutor, const std::string &descriptionFormat,
    const std::vector<std::string> &descriptionArgs)
    : runnable_(runnable), priority_(priority), descriptionFormat_(descriptionFormat),
      descriptionArgs_(descriptionArgs), actionExecutor_(actionExecutor)
{
    if (!runnable_) {
        throw std::invalid_argument("runnable cannot be null");
    }

    // {
    // }
}

Mail::Mail(std::shared_ptr<ThrowingRunnable> runnable, int priority, const std::string &descriptionFormat,
    const std::vector<std::string> &descriptionArgs)
    : Mail(runnable, priority, StreamTaskActionExecutor::IMMEDIATE, descriptionFormat, descriptionArgs)
{}

Mail::~Mail()
{}

int Mail::getPriority() const
{
    return priority_;
}

void Mail::tryCancel()
{
    runnable_->tryCancel();
}

std::string Mail::toString() const
{
    std::stringstream ss;
    ss << descriptionFormat_;
    if (!descriptionArgs_.empty()) {
        size_t argIndex = 0;
        size_t formatIndex = 0;
        while (formatIndex < descriptionFormat_.size()) {
            if (descriptionFormat_[formatIndex] == '%' && argIndex < descriptionArgs_.size()) {
                ss << descriptionArgs_[argIndex++];
                formatIndex++;
            } else {
                ss << descriptionFormat_[formatIndex++];
            }
        }
    }
    return ss.str();
}

void Mail::run()
{
    actionExecutor_->run(runnable_.get());
}
}  // namespace omnistream
