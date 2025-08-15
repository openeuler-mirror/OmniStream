/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/10/25.
//

#ifndef JOBVERTEXID_H
#define JOBVERTEXID_H
#include <utils/AbstractID.h>

namespace omnistream {
    class JobVertexID : public AbstractID {
    public:
            JobVertexID(long lowerpart, long upperpart)
                : AbstractID(lowerpart, upperpart) {
            }
    };
}


#endif //JOBVERTEXID_H
