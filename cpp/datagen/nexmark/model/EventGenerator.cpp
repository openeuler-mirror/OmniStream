/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "EventGenerator.h"

// Definition and initialization of the static member REUSABLE_EXTRA_STRING.
std::string StringsGenerator::REUSABLE_EXTRA_STRING = []() -> std::string {
    std::string str;
    str.resize(1024 * 1024);
    SplittableRandom random(0);
    StringsGenerator::nextExactString(random, 1024 * 1024, str.data());
    return str;
}();

std::string_view StringsGenerator::nextExactString(SplittableRandom &random, size_t length, char *buffer)
{
    if (!REUSABLE_EXTRA_STRING.empty() && length < REUSABLE_EXTRA_STRING.size() / 2) {
        int offset = random.nextInt(REUSABLE_EXTRA_STRING.size() - length);
        return std::string_view(REUSABLE_EXTRA_STRING.data() + offset, length);
        // return REUSABLE_EXTRA_STRING.substr(offset, length);
    }
    StringsGenerator::fillWithRandomLower<false>(random, buffer, length);
    return std::string_view(buffer, length);
}

std::string StringsGenerator::nextString(SplittableRandom &random, int maxLength, char special)
{
    int len = MIN_STRING_LENGTH + random.nextInt(maxLength - MIN_STRING_LENGTH);
    std::string sb;
    while (len-- > 0) {
        if (random.nextInt(13) == 0) {
            sb.push_back(special);
        } else {
            sb.push_back(static_cast<char>('a' + random.nextInt(26)));
        }
    }
    return trim(sb);
}

std::string_view StringsGenerator::nextExtra(
    SplittableRandom &random, int currentSize, int desiredAverageSize, char *buffer)
{
    if (currentSize > desiredAverageSize) {
        return "";
    }
    desiredAverageSize -= currentSize;
    int delta = static_cast<int>(std::round(desiredAverageSize * 0.2));
    int minSize = desiredAverageSize - delta;
    int desiredSize = minSize + (delta == 0 ? 0 : random.nextInt(2 * delta));
    return nextExactString(random, desiredSize, buffer);
}

std::string trim(const std::string &s)
{
    auto start = std::find_if_not(s.begin(), s.end(), ::isspace);
    auto end = std::find_if_not(s.rbegin(), s.rend(), ::isspace).base();
    return (start < end) ? std::string(start, end) : std::string();
}
