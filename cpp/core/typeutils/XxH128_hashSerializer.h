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

#ifndef OMNISTREAM_XXH128_HASHSERIALIZER_H
#define OMNISTREAM_XXH128_HASHSERIALIZER_H

#include <xxhash.h>
#include "TypeSerializerSingleton.h"

class XxH128_hashSerializer : public TypeSerializerSingleton {
public:
    XxH128_hashSerializer();

    void *deserialize(DataInputView &source) override;
    void serialize(void *record, DataOutputSerializer &target) override;

    static XxH128_hashSerializer* instance;

    BackendDataType getBackendId() const override { return BackendDataType::XXHASH128_BK;};
};


#endif // OMNISTREAM_XXH128_HASHSERIALIZER_H
