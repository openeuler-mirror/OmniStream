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

#endif
