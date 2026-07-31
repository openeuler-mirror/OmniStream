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

#ifndef FLINK_TNEL_ORG_SLF4J_LOGGERFACTORY_H
#define FLINK_TNEL_ORG_SLF4J_LOGGERFACTORY_H
#include "basictypes/Object.h"
#include "org_slf4j_Logger.h"
#include "basictypes/Class.h"
class LoggerFactory : public Object {
public:
    LoggerFactory();
    ~LoggerFactory();
    static Logger* getLogger(Class* cls);
};
#endif // FLINK_TNEL_ORG_SLF4J_LOGGERFACTORY_H
