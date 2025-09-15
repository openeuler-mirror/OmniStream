/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SERIALIZATIONDELEGATE_H
#define FLINK_TNEL_SERIALIZATIONDELEGATE_H
#include <memory>
#include "../io/IOReadableWritable.h"
#include "../typeutils/TypeSerializer.h"

class SerializationDelegate : public IOReadableWritable {
public:
    explicit SerializationDelegate(std::unique_ptr<TypeSerializer>serializer);
    Object *getInstance() const;
    void setInstance(Object *instance);

    void write(DataOutputSerializer &out) override;

    void read(DataInputView &in) override;

private:
    Object *instance_;
    std::unique_ptr<TypeSerializer> serializer_;
};
#endif //FLINK_TNEL_SERIALIZATIONDELEGATE_H
