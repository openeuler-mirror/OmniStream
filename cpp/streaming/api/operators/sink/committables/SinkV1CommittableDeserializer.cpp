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

#include "SinkV1CommittableDeserializer.h"
#include "connector/kafka/sink/KafkaCommittable.h"
#include "core/io/SimpleVersionedSerialization.h"

void SinkV1CommittableDeserializer::validateMagicNumber(DataInputDeserializer& in) {
    const int magicNumber = in.readInt();
    if (magicNumber != MAGIC_NUMBER) {
        throw std::runtime_error("Corrupt data: Unexpected magic number");
    }
}

template <typename T>
std::vector<T>* SinkV1CommittableDeserializer::readVersionAndDeserializeList(
    std::shared_ptr<SimpleVersionedSerializer<T>>& serializer, DataInputDeserializer& in) {
    validateMagicNumber(in);
    return SimpleVersionedSerialization::readVersionAndDeserializeList(*serializer, in);
}

template std::vector<KafkaCommittable>*
SinkV1CommittableDeserializer::readVersionAndDeserializeList<KafkaCommittable>(
    std::shared_ptr<SimpleVersionedSerializer<KafkaCommittable>>&,
    DataInputDeserializer&);