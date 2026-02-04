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
#ifndef OMNISTREAM_STATUSWATERMARKVALVE_H
#define OMNISTREAM_STATUSWATERMARKVALVE_H

#include "functions/StreamElement.h"
#include "common.h"
#include "WatermarkStatus.h"

struct InputChannelStatus{
    int64_t watermark;
    bool isActive;
    InputChannelStatus(): watermark(INT64_MIN),isActive(true){}
};

class StatusWatermarkValve{
    private:
        std::vector<InputChannelStatus> channelStatuses;
        int64_t lastOutputWatermark;

    public:
        int64_t inputWatermark(Watermark* watermark, int channelIndex){
            int64_t timestamp = watermark->getTimeStamp();
            if (timestamp > channelStatuses[channelIndex].watermark) {
                channelStatuses[channelIndex].watermark = timestamp;
            }
            int64_t newMin = INT64_MAX;
            for(const auto& status: channelStatuses){
                if(!status.isActive) continue;
                if(status.watermark < newMin){
                    newMin = status.watermark;
                }
            }
            if (newMin> lastOutputWatermark){
                lastOutputWatermark = newMin;
                return newMin;
            }
            return lastOutputWatermark;
        }

};
#endif // OMNISTREAM_STATUSWATERMARKVALVE_H