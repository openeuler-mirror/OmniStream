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
#ifndef FLINK_TNEL_PROCESSINGTIMESERVICEUTIL_H
#define FLINK_TNEL_PROCESSINGTIMESERVICEUTIL_H

#include <atomic>

class ProcessingTimeServiceUtil {
public:
    static int64_t getProcessingTimeDelay(int64_t processingTimestamp, int64_t currentTimestamp)
    {
        if (processingTimestamp >= currentTimestamp) {
            return processingTimestamp - currentTimestamp + 1;
        } else {
            return 0;
        }
    }
};
#endif // FLINK_TNEL_PROCESSINGTIMESERVICEUTIL_H
