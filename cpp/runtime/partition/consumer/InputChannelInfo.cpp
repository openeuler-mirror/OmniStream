/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

    InputChannelInfo& InputChannelInfo::operator=(const InputChannelInfo& other) {
        if (this != &other) {
            gateIdx = other.gateIdx;
            inputChannelIdx = other.inputChannelIdx;
        }
        return *this;
    }

    InputChannelInfo::~InputChannelInfo() {}

    int InputChannelInfo::getGateIdx() const {
        return gateIdx;
    }

    void InputChannelInfo::setGateIdx(int gateIdx) {
        this->gateIdx = gateIdx;
    }

    int InputChannelInfo::getInputChannelIdx() const {
        return inputChannelIdx;
    }

    void InputChannelInfo::setInputChannelIdx(int inputChannelIdx) {
        this->inputChannelIdx = inputChannelIdx;
    }

    bool InputChannelInfo::operator==(const InputChannelInfo& other) const {
        return gateIdx == other.gateIdx && inputChannelIdx == other.inputChannelIdx;
    }

    bool InputChannelInfo::operator!=(const InputChannelInfo& other) const {
        return !(*this == other);
    }

    std::string InputChannelInfo::toString() const {
        std::stringstream ss;
        ss << "InputChannelInfo{gateIdx=" << gateIdx << ", inputChannelIdx=" << inputChannelIdx << "}";
        return ss.str();
    }

} // namespace omnistream