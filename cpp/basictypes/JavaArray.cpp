/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/JavaArray.h"

template <typename T>
void JavaArray<T>::set(int index, T value)
{
    if (index >= length) {
        throw std::out_of_range("Index out of range");
    }
    data_[index] = value;
}

template <typename T>
T JavaArray<T>::get(int index)
{
    if (index >= length) {
        throw std::out_of_range("Index out of range");
    }
    return data_[index];
}

// short、int、long、float、double、bool、char
template <typename T>
bool JavaArray<T>::equals(Object *obj)
{
    JavaArray *a = (JavaArray *)obj;
    return (*this == *a);
}

template <typename T>
Object* JavaArray<T>::clone()
{
    int length = this->length;
    JavaArray *a = new JavaArray(length);
    errno_t ret = memcpy_s((void *)a->data_, sizeof(T)*length, (void *)this->data_, sizeof(T)*length);
    if (ret != 0) {
        throw std::runtime_error("memcpy_s failed" + std::to_string(ret));
    }
    return (Object*)a;
}

template class JavaArray<long>;
template class JavaArray<int>;
template class JavaArray<short>;
template class JavaArray<char>;
template class JavaArray<float>;
template class JavaArray<double>;
template class JavaArray<bool>;
