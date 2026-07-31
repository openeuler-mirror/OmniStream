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

#include "KafkaWriterState.h"
#include <stdexcept>
#include <functional>

KafkaWriterState::KafkaWriterState(const std::string& transactionalIdPrefix)
{
    if (transactionalIdPrefix.empty()) {
        throw std::invalid_argument("transactionalIdPrefix");
    }
    this->transactionalIdPrefix = transactionalIdPrefix;
}

std::string KafkaWriterState::getTransactionalIdPrefix() const
{
    return transactionalIdPrefix;
}

bool KafkaWriterState::operator==(const KafkaWriterState& that) const
{
    return this == &that || transactionalIdPrefix == that.transactionalIdPrefix;
}

bool KafkaWriterState::operator!=(const KafkaWriterState& that) const
{
    return !(*this == that);
}

size_t KafkaWriterState::hashCode() const
{
    return std::hash<std::string>{}(transactionalIdPrefix);
}

std::string KafkaWriterState::toString() const
{
    return "KafkaWriterState{, transactionalIdPrefix='" + transactionalIdPrefix + "'}";
}
