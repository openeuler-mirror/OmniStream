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

#ifndef OMNISTREAM_BINARYTYPEINFO_H
#define OMNISTREAM_BINARYTYPEINFO_H

#include "TypeInformation.h"
#include "table/typeutils/BinaryRowDataSerializer.h"

class BinaryTypeInfo : public TypeInformation {
public:
    BinaryTypeInfo(int numFields, const std::vector<std::string>& inputTypes)
    {
        typeSerializer_ = new BinaryRowDataSerializer(numFields, inputTypes);
    }

    std::string name() override
    {
        return typeSerializer_->getName();
    }

    BackendDataType getBackendId() const override
    {
        return typeSerializer_->getBackendId();
    }

    TypeSerializer* createTypeSerializer() override
    {
        return typeSerializer_;
    }

    static BinaryTypeInfo* of(int numFields, const std::vector<std::string>& inputTypes)
    {
        return new BinaryTypeInfo(numFields, inputTypes);
    }

private:
    TypeSerializer* typeSerializer_;
};
#endif // OMNISTREAM_BINARYTYPEINFO_H
