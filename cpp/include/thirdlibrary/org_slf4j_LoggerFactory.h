/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/23/25.
//

#ifndef FLINK_TNEL_ORG_SLF4J_LOGGERFACTORY_H
#define FLINK_TNEL_ORG_SLF4J_LOGGERFACTORY_H
#include "basictypes/Object.h"
#include "org_slf4j_Logger.h"
#include "basictypes/Class.h"
class LoggerFactory : public Object {
public:
    LoggerFactory();
    ~LoggerFactory();
    static Logger* getLogger(Class * cls);
};
#endif //FLINK_TNEL_ORG_SLF4J_LOGGERFACTORY_H
