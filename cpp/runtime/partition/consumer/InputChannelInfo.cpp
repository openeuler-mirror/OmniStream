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
// InputChannelInfo.cpp
#include "InputChannelInfo.h"
#include <sstream>

namespace omnistream {

    InputChannelInfo::InputChannelInfo() : gateIdx(0), inputChannelIdx(0) {}

    InputChannelInfo::InputChannelInfo(int gateIdx, int inputChannelIdx)
        : gateIdx(gateIdx), inputChannelIdx(inputChannelIdx) {}

    InputChannelInfo::InputChannelInfo(const InputChannelInfo& other)
        : gateIdx(other.gateIdx), inputChannelIdx(other.inputChannelIdx) {}

    InputChannelInfo& InputChannelInfo::operator=(const InputChannelInfo& other)
    {
        if (this != &other) {
            gateIdx = other.gateIdx;
            inputChannelIdx = other.inputChannelIdx;
        }
        return *this;
    }

    InputChannelInfo::~InputChannelInfo() {}

    int InputChannelInfo::getGateIdx() const
    {
        return gateIdx;
    }

    void InputChannelInfo::setGateIdx(int gateIdx_)
    {
        this->gateIdx = gateIdx_;
    }

    void InputChannelInfo::setOmni()
    {
        this->omni = true;
    }

    bool InputChannelInfo::getOmni()
    {
        return omni;
    }

    int InputChannelInfo::getInputChannelIdx() const
    {
        return inputChannelIdx;
    }

    void InputChannelInfo::setInputChannelIdx(int inputChannelIdx_)
    {
        this->inputChannelIdx = inputChannelIdx_;
    }

    bool InputChannelInfo::operator==(const InputChannelInfo& other) const
    {
        return gateIdx == other.gateIdx && inputChannelIdx == other.inputChannelIdx;
    }

    bool InputChannelInfo::operator!=(const InputChannelInfo& other) const
    {
        return !(*this == other);
    }
    bool InputChannelInfo::operator<(const InputChannelInfo& other) const
    {
        if (gateIdx != other.gateIdx) {
            return gateIdx < other.gateIdx;
        }
        return inputChannelIdx < other.inputChannelIdx;
    }

    std::string InputChannelInfo::toString() const
    {
        std::stringstream ss;
        ss << "InputChannelInfo{gateIdx=" << gateIdx << ", inputChannelIdx=" << inputChannelIdx << "}";
        return ss.str();
    }

} // namespace omnistream