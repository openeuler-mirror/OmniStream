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
#include "basictypes/Tuple2.h"

Tuple2::Tuple2(Object *f0, Object *f1)
{
    this->f0 = f0;
    f0->getRefCount();
    this->f1 = f1;
    f1->getRefCount();
}

Tuple2::Tuple2()
{
    f0 = nullptr;
    f1 = nullptr;
}

Tuple2::~Tuple2()
{
    if (f0 != nullptr) {
        f0->putRefCount();
    }
    if (f1 != nullptr) {
        if (!f1->isPool) {
            f1->putRefCount();
        } else {
            ((Long *)f1)->putRefCount();
        }
    }
}

void Tuple2::SetF0(Object *obj)
{
    if (f0 != obj) {
        if (obj != nullptr) {
            obj->getRefCount();
        }
        if (f0 != nullptr) {
            f0->putRefCount();
        }
        f0 = obj;
    }
}

Object* Tuple2::GetF0()
{
    return f0;
}

void Tuple2::SetF1(Object *obj)
{
    if (f1 != obj) {
        if (obj != nullptr) {
            obj->getRefCount();
        }
        if (f1 != nullptr) {
            f1->putRefCount();
        }
        f1 = obj;
    }
}

Object* Tuple2::GetF1()
{
    return f1;
}

int Tuple2::hashCode()
{
    int f0Hash = f0->hashCode();
    int f1Hash = f1->hashCode();
    return (int)(f0Hash * 31 + f1Hash);
}

bool Tuple2::equals(Object *obj)
{
    Tuple2 *tuple = reinterpret_cast<Tuple2 *>(obj);
    return f0->equals(tuple->GetF0()) && f1->equals(tuple->GetF1());
}

Object* Tuple2::clone()
{
    auto k0 = f0->clone();
    auto k1 = f1->clone();
    auto cloned = new Tuple2(k0, k1);
    k0->putRefCount();
    k1->putRefCount();
    return cloned;
}

Tuple2* Tuple2::of(Object *f0, Object *f1)
{
    Tuple2* tuple = new Tuple2(f0, f1);
    return tuple;
}
