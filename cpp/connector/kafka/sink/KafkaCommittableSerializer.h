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

#ifndef OMNIFLINK_KAFKACOMMITTABLESERIALIZER_H
#define OMNIFLINK_KAFKACOMMITTABLESERIALIZER_H

#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include "KafkaCommittable.h"

class KafkaCommittableSerializer {
public:
    int GetVersion();
    std::vector<char> serialize(KafkaCommittable* state);
    KafkaCommittable* deserialize(int version, const std::vector<char>& serialized);
};

#endif // OMNIFLINK_KAFKACOMMITTABLESERIALIZER_H
