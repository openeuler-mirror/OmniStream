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

#ifndef AVAILABILITYWITHBACKLOG_H
#define AVAILABILITYWITHBACKLOG_H

#include <string>

namespace omnistream {

    class AvailabilityWithBacklog {
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
    private:
        bool isAvailable;
        int backlog;
    };

} // namespace omnistream

#endif // AVAILABILITYWITHBACKLOG_H
