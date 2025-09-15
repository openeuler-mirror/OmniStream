/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef ABSTRACTID_H
#define ABSTRACTID_H


namespace omnistream {
    class AbstractID {

    public:
        AbstractID(long lowerpart, long upperpart) : lowerpart_(lowerpart), upperpart_(upperpart) {};
        virtual ~AbstractID() = default;

    protected:
        long lowerpart_;
        long upperpart_;
    };
}


#endif  //ABSTRACTID_H
