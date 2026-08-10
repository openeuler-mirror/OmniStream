/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#include "SimpleCounter.h"

#include <mutex>
#include <unordered_set>

namespace omnistream {
namespace {
// Function-local statics so there is no static initialisation order dependency: a counter can be
// constructed from any translation unit, including during library load.
std::mutex &LiveMutex()
{
    static std::mutex mutex;
    return mutex;
}

std::unordered_set<const SimpleCounter *> &LiveSet()
{
    static std::unordered_set<const SimpleCounter *> live;
    return live;
}
} // namespace

SimpleCounter::SimpleCounter() : count(0)
{
    std::lock_guard<std::mutex> guard(LiveMutex());
    LiveSet().insert(this);
}

SimpleCounter::~SimpleCounter()
{
    std::lock_guard<std::mutex> guard(LiveMutex());
    LiveSet().erase(this);
}

bool SimpleCounter::IsLive(const SimpleCounter *counter)
{
    if (counter == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> guard(LiveMutex());
    return LiveSet().find(counter) != LiveSet().end();
}

void SimpleCounter::Inc()
{
    ++count;
}

void SimpleCounter::Inc(long n)
{
    count += n;
}

void SimpleCounter::Dec()
{
    --count;
}

void SimpleCounter::Dec(long n)
{
    count -= n;
}

long SimpleCounter::GetCount()
{
    return count;
}
} // namespace omnistream
