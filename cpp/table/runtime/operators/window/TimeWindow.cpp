/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "../../core/io/DataInputView.h"
#include "TimeWindow.h"

TimeWindow::TimeWindow() {}

TimeWindow::TimeWindow(long start, long end) : start(start), end(end) {}

long TimeWindow::getStart() const
{
    return this->start;
}

long TimeWindow::getEnd() const
{
    return this->end;
}

long TimeWindow::maxTimestamp() const
{
    return this->end - 1L;
}

bool TimeWindow::intersects(const TimeWindow &other) const
{
    return this->start <= other.end && this->end >= other.start;
}

TimeWindow TimeWindow::cover(const TimeWindow &other) const
{
    return {std::min(this->start, other.start), std::max(this->end, other.end)};
}

long TimeWindow::getWindowStartWithOffset(long timestamp, long offset, long windowSize)
{
    long remainder = (timestamp - offset) % windowSize;
    return remainder < 0L ? timestamp - (remainder + windowSize) : timestamp - remainder;
}

TimeWindow *TimeWindow::of(long start_, long end_)
{
    return new TimeWindow(start_, end_);
}

bool TimeWindow::operator<(const TimeWindow &other) const
{
    if (start == other.start) {
        return end < other.end;
    }
    return start < other.start;
}

bool TimeWindow::operator>(const TimeWindow &other) const
{
    if (start == other.start) {
        return end > other.end;
    }
    return start > other.start;
}

std::ostream &operator<<(std::ostream &os, const TimeWindow &obj)
{
    os << "TimeWindow{start=" << std::to_string(obj.start) << ", end=" << std::to_string(obj.end) << '}';
    return os;
}

TimeWindow::Serializer::Serializer() {}

bool TimeWindow::Serializer::isImmutableType() const
{
    return true;
}

TimeWindow *TimeWindow::Serializer::createInstance() const
{
    return new TimeWindow(0, 1);
}

TimeWindow *TimeWindow::Serializer::copy(TimeWindow *from) const
{
    return from;
}

TimeWindow *TimeWindow::Serializer::copy(TimeWindow *from, TimeWindow *reuse) const
{
    return from;
}

int TimeWindow::Serializer::getLength() const
{
    return sizeof(int64_t) * 2;
}

void TimeWindow::Serializer::serialize(void *input, DataOutputSerializer &target)
{
    TimeWindow *record = (TimeWindow *) input;
    target.writeLong(record->start);
    target.writeLong(record->end);
}

void *TimeWindow::Serializer::deserialize(DataInputView &source)
{
    long start1;
    long end1;
    start1 = source.readLong();
    end1 = source.readLong();
    return new TimeWindow(start1, end1);
}

void TimeWindow::Serializer::copy(DataInputView *source, DataOutputSerializer *target) const
{
    target->writeLong(source->readLong());
    target->writeLong(source->readLong());
}
BackendDataType TimeWindow::Serializer::getBackendId() const
{
    return BackendDataType::TIME_WINDOW_BK;
}
