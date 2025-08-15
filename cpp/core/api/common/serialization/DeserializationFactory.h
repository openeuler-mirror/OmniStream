/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_DESERIALIZATIONFACTORY_H
#define OMNISTREAM_DESERIALIZATIONFACTORY_H

#include "DeserializationSchema.h"

class DeserializationFactory {
public:
    static DeserializationSchema* getDeserializationSchema(nlohmann::json& opDescriptionJSON);
};


#endif  //OMNISTREAM_DESERIALIZATIONFACTORY_H
