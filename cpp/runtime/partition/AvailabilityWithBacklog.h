/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/25/25.
//

// AvailabilityWithBacklog.h
#ifndef AVAILABILITYWITHBACKLOG_H
#define AVAILABILITYWITHBACKLOG_H

#include <string>

namespace omnistream {

    class AvailabilityWithBacklog {
    private:
        bool isAvailable;
        int backlog;

    public:
        AvailabilityWithBacklog();
        AvailabilityWithBacklog(bool isAvailable, int backlog);
        AvailabilityWithBacklog(const AvailabilityWithBacklog& other);
        AvailabilityWithBacklog& operator=(const AvailabilityWithBacklog& other);
        bool operator==(const AvailabilityWithBacklog& other) const;
        bool operator!=(const AvailabilityWithBacklog& other) const;
        ~AvailabilityWithBacklog();

        bool getIsAvailable() const;
        int getBacklog() const;

        void setIsAvailable(bool isAvailable);
        void setBacklog(int backlog);

        std::string toString() const;
    };

} // namespace omnistream

#endif // AVAILABILITYWITHBACKLOG_H
