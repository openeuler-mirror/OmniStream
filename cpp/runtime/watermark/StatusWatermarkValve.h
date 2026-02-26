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

#include "common.h"
#include "WatermarkStatus.h"
#include <optional>

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
        StatusWatermarkValve(int numInputChannels) {
            channelStatuses.resize(numInputChannels);
            lastOutputWatermark = INT64_MIN;
        }
        std::optional<long> inputWatermark(Watermark* watermark, int channelIndex){
            if (watermark == nullptr) {
                return std::nullopt; 
            }
            int64_t timestamp = watermark->getTimestamp();
            if(channelIndex<0 || channelIndex>=channelStatuses.size() ){
                std::cerr << "WARNING: Ignored invalid channel index: " << channelIndex << std::endl;
                return timestamp;
            }
            if (timestamp > channelStatuses[channelIndex].watermark) {
                channelStatuses[channelIndex].watermark = timestamp;
            }
            int64_t newMin = INT64_MAX;
            bool allIdle = true;
            for(const auto& status: channelStatuses){
                if(!status.isActive) continue;
                allIdle = false;
                if(status.watermark < newMin){
                    newMin = status.watermark;
                }
            }
            if (allIdle) {
                return lastOutputWatermark;
            }
            if (newMin> lastOutputWatermark){
                lastOutputWatermark = newMin;
                return lastOutputWatermark;
            }
            return lastOutputWatermark;
        }

};
#endif // OMNISTREAM_STATUSWATERMARKVALVE_H