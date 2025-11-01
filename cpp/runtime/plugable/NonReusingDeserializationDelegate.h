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

#ifndef FLINK_TNEL_NONREUSINGDESERIALIZATIONDELEGATE_H
#define FLINK_TNEL_NONREUSINGDESERIALIZATIONDELEGATE_H

#include <memory>
#include "DeserializationDelegate.h"
#include "core/typeutils/TypeSerializer.h"

class NonReusingDeserializationDelegate : public DeserializationDelegate {
public:
    explicit NonReusingDeserializationDelegate(std::unique_ptr<TypeSerializer> serializer);
    void* getInstance() override;
    void setInstance(void* instance) override;
    void write(DataOutputSerializer& out) override;
    void read(DataInputView& in) override;
private:
    std::unique_ptr<TypeSerializer> serializer_;
    void* instance_;
};


#endif
