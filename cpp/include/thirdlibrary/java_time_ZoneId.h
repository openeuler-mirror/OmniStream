/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_ZoneId_H
#define FLINK_TNEL_ZoneId_H

#include <string>
#include "basictypes/String.h"

class ZoneId : public Object {
public:
    ZoneId();

    ZoneId(std::string& tz);

    ~ZoneId();

    static ZoneId *systemDefault();

    std::string getTimezone();

private:
    std::string timezone;
};

#endif //FLINK_TNEL_ZoneId_H
