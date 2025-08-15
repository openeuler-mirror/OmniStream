/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/25/25.
//

#include "AvailabilityWithBacklog.h"
// AvailabilityWithBacklog.cpp
#include <sstream>
#include <stdexcept>
#include "AvailabilityWithBacklog.h"

namespace omnistream {

    AvailabilityWithBacklog::AvailabilityWithBacklog() : isAvailable(false), backlog(0) {}

    AvailabilityWithBacklog::AvailabilityWithBacklog(bool isAvailable, int backlog) : isAvailable(isAvailable), backlog(backlog) {
        if (backlog < 0) {
            throw std::invalid_argument("Backlog must be non-negative.");
        }
    }

    AvailabilityWithBacklog::AvailabilityWithBacklog(const AvailabilityWithBacklog& other) : isAvailable(other.isAvailable), backlog(other.backlog) {}

    AvailabilityWithBacklog& AvailabilityWithBacklog::operator=(const AvailabilityWithBacklog& other) {
        if (this != &other) {
            isAvailable = other.isAvailable;
            backlog = other.backlog;
        }
        return *this;
    }

    bool AvailabilityWithBacklog::operator==(const AvailabilityWithBacklog& other) const {
        return isAvailable == other.isAvailable && backlog == other.backlog;
    }

    bool AvailabilityWithBacklog::operator!=(const AvailabilityWithBacklog& other) const {
        return !(*this == other);
    }

    AvailabilityWithBacklog::~AvailabilityWithBacklog() {}

    bool AvailabilityWithBacklog::getIsAvailable() const {
        return isAvailable;
    }

    int AvailabilityWithBacklog::getBacklog() const {
        return backlog;
    }

    void AvailabilityWithBacklog::setIsAvailable(bool isAvailable) {
        this->isAvailable = isAvailable;
    }

    void AvailabilityWithBacklog::setBacklog(int backlog) {
        if (backlog < 0) {
            throw std::invalid_argument("Backlog must be non-negative.");
        }
        this->backlog = backlog;
    }

    std::string AvailabilityWithBacklog::toString() const {
        std::stringstream ss;
        ss << "AvailabilityWithBacklog{isAvailable=" << (isAvailable ? "true" : "false") << ", backlog=" << backlog << "}";
        return ss.str();
    }

} // namespace omnistream
