/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/24/25.
//

#ifndef FLINK_TNEL_PARAMETERTOOL_H
#define FLINK_TNEL_PARAMETERTOOL_H
// todo just a stub, need to be implemented
#include "basictypes//Object.h"
#include "basictypes/java_io_InputStream.h"
#include "functions/Configuration.h"
class ParameterTool : public Object {
public:
    ParameterTool();
    ~ParameterTool();
    static ParameterTool* fromPropertiesFile(InputStream * inputStream);
    Configuration* getConfiguration();
};
#endif //FLINK_TNEL_PARAMETERTOOL_H
