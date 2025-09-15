/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef AUTOCLOSEABLE_H
#define AUTOCLOSEABLE_H

namespace omnistream
{
    class AutoCloseable
    {
    public:
        virtual ~AutoCloseable() = default;
        virtual  void close() = 0 ;
    };
}


#endif  //AUTOCLOSEABLE_H
