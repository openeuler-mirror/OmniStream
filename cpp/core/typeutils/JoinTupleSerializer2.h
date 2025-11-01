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

#ifndef OMNISTREAM_JOINTUPLESERIALIZER2_H
#define OMNISTREAM_JOINTUPLESERIALIZER2_H

#include "TypeSerializerSingleton.h"


class JoinTupleSerializer2 : public TypeSerializerSingleton {
public:
    JoinTupleSerializer2();

    void *deserialize(DataInputView &source) override;
    void serialize(void *record, DataOutputSerializer &target) override;

    static JoinTupleSerializer2* instance;

    BackendDataType getBackendId() const override { return BackendDataType::TUPLE_INT32_INT32_INT64;};
};


#endif // OMNISTREAM_JOINTUPLESERIALIZER_H
