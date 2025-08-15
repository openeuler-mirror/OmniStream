/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// InputChannelInfo.h
#ifndef INPUTCHANNELINFO_H
#define INPUTCHANNELINFO_H

#include <string>

namespace omnistream {

    class InputChannelInfo {
    public:
        InputChannelInfo();
        InputChannelInfo(int gateIdx, int inputChannelIdx);
        InputChannelInfo(const InputChannelInfo& other);
        InputChannelInfo& operator=(const InputChannelInfo& other);
        ~InputChannelInfo();

        int getGateIdx() const;
        void setGateIdx(int gateIdx);

        int getInputChannelIdx() const;
        void setInputChannelIdx(int inputChannelIdx);

        bool operator==(const InputChannelInfo& other) const;
        bool operator!=(const InputChannelInfo& other) const;

        std::string toString() const;

    private:
        int gateIdx;
        int inputChannelIdx;
    };

} // namespace omnistream

#endif // INPUTCHANNELINFO_H