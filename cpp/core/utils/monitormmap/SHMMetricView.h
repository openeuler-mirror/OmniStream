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

#ifndef SHMMETRICVIEW_H
#define SHMMETRICVIEW_H


namespace omnistream {
    class SHMMetricView {
    public:
        explicit SHMMetricView(long int* valuePtr)
            : valuePtr_(valuePtr)
        {
        }

        ~SHMMetricView();

        long int GetThreadID();
        long int GetProbeID();
        long int GetMilliSeconds();
        long int GetCount();

    private:
        const int threadIDOffset = 0;
        const int probeIDOffset = 1;
        const int millisecondsOffset = 2;
        const int countOffset = 3;
        long int *valuePtr_;
    };
}

#endif // SHMMETRICVIEW_H
