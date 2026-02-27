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

#ifndef FLINK_TNEL_SERIALIZATIONDELEGATE_H
#define FLINK_TNEL_SERIALIZATIONDELEGATE_H
#include <memory>
#include "core/io/IOReadableWritable.h"
#include "core/typeutils/TypeSerializer.h"

class SerializationDelegate : public IOReadableWritable {
public:
    explicit SerializationDelegate(TypeSerializer* serializer);

    ~SerializationDelegate() override;

    Object *getInstance() const;
    void setInstance(Object *instance);

    void write(DataOutputSerializer &out) override;

    void read(DataInputView &in) override;

private:
    Object *instance_;
    TypeSerializer* serializer_;
};
#endif
