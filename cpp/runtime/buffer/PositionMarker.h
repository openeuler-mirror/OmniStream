/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/26/25.
//


#ifndef OMNISTREAM_POSITION_MARKER_H
#define OMNISTREAM_POSITION_MARKER_H

#include <string>
#include <sstream>
#include <cmath>

namespace omnistream {

class PositionMarker {
public:
    static const int FINISHED_EMPTY = std::numeric_limits<int>::min();

    virtual int get() const = 0;

    static bool isFinished(int position) {
        return position < 0;
    }

    static int getAbsolute(int position) {
        if (position == FINISHED_EMPTY) {
            return 0;
        }
        return std::abs(position);
    }

    virtual ~PositionMarker() = default;
};

class SettablePositionMarker : public PositionMarker {
public:
    SettablePositionMarker() : position(0), cachedPosition(0) {}
    SettablePositionMarker(const SettablePositionMarker& other) : position(other.position), cachedPosition(other.cachedPosition) {}
    SettablePositionMarker& operator=(const SettablePositionMarker& other) {
        if (this != &other) {
            position = other.position;
            cachedPosition = other.cachedPosition;
        }
        return *this;
    }

    int get() const override {
        return position;
    }

    bool isFinished() const {
        return PositionMarker::isFinished(cachedPosition);
    }

    int getCached() const {
        return PositionMarker::getAbsolute(cachedPosition);
    }

    int markFinished() {
        int currentPosition = getCached();
        int newValue = -currentPosition;
        if (newValue == 0) {
            newValue = FINISHED_EMPTY;
        }
        set(newValue);
        return currentPosition;
    }

    void move(int offset) {
        set(cachedPosition + offset);
    }

    void set(int value) {
        cachedPosition = value;
    }

    void commit() {
        position = cachedPosition;
    }

    std::string toString() const {
        std::stringstream ss;
        ss << "SettablePositionMarker{position=" << position << ", cachedPosition=" << cachedPosition << "}";
        return ss.str();
    }

    ~SettablePositionMarker() override = default;

private:
    int position;
    int cachedPosition;
};

} // namespace omnistream

#endif // OMNISTREAM_POSITION_MARKER_H
