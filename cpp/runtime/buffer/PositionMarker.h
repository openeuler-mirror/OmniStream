/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

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

    static bool isFinished(int position)
    {
        return position < 0;
    }

    static int getAbsolute(int position)
    {
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
    SettablePositionMarker& operator=(const SettablePositionMarker& other)
    {
        if (this != &other) {
            position = other.position;
            cachedPosition = other.cachedPosition;
        }
        return *this;
    }

    int get() const override
    {
        return position;
    }

    bool isFinished() const
    {
        return PositionMarker::isFinished(cachedPosition);
    }

    int getCached() const
    {
        return PositionMarker::getAbsolute(cachedPosition);
    }

    int markFinished()
    {
        int currentPosition = getCached();
        int newValue = -currentPosition;
        if (newValue == 0) {
            newValue = FINISHED_EMPTY;
        }
        set(newValue);
        return currentPosition;
    }

    void move(int offset)
    {
        set(cachedPosition + offset);
    }

    void set(int value)
    {
        cachedPosition = value;
    }

    void commit()
    {
        position = cachedPosition;
    }

    std::string toString() const
    {
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
