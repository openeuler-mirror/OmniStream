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

#include "KafkaCommittableSerializer.h"
#include "core/memory/DataInputDeserializer.h"

int KafkaCommittableSerializer::getVersion() const
{
    return 1;
}

std::vector<uint8_t> KafkaCommittableSerializer::serialize(const KafkaCommittable&  obj ) { return {}; }

KafkaCommittable* KafkaCommittableSerializer::deserialize(int version, std::vector<uint8_t>&  serialized)
{
    DataInputDeserializer deserializer;
    deserializer.setBuffer(serialized.data(), serialized.size(), 0, serialized.size());
    short epoch = deserializer.readUnsignedShort();
    long producerId = deserializer.readLong();
    std::string transactionalId = deserializer.readUTF();
    return new KafkaCommittable(producerId, epoch, transactionalId, nullptr);
}