/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef TYPESERIALIZER_NEW_H
#define TYPESERIALIZER_NEW_H

template <typename T>
class TypeSerializer {
public:
    virtual bool isImmutableType() = 0;
    virtual TypeSerializer<T> duplicate() = 0;
    virtual T createInstance() = 0;
    virtual T copy(T *from) = 0;
    virtual T copy(T *from, T *reuse) = 0;
};

#endif //TYPESERIALIZER_NEW_H
