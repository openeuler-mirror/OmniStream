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
#include "CheckpointStorageLocationReference.h"

CheckpointStorageLocationReference::CheckpointStorageLocationReference(
    std::vector<uint8_t> *encodedReference) : encodedReference_(encodedReference)
{
    if (encodedReference == nullptr || encodedReference->empty()) {
        throw std::invalid_argument("encodedReference must not be empty");
    }
}

CheckpointStorageLocationReference::CheckpointStorageLocationReference() : encodedReference_(nullptr)
{
}

CheckpointStorageLocationReference::~CheckpointStorageLocationReference()
{
    delete encodedReference_;
}

std::vector<uint8_t> *CheckpointStorageLocationReference::GetReferenceBytes()
    const
{
    return encodedReference_ != nullptr ? encodedReference_
                                        : new std::vector<uint8_t>();
}

bool CheckpointStorageLocationReference::IsDefaultReference() const
{
    return encodedReference_ == nullptr;
}

int CheckpointStorageLocationReference::HashCode() const
{
    if (this->encodedReference_ == nullptr) {
        return 2059243550;
    } else {
        int result = 1;
        auto var2 = this->encodedReference_;
        size_t var3 = this->encodedReference_->size();

        for (size_t var4 = 0; var4 < var3; ++var4) {
            uint8_t element = var2->at(var4);
            result = 31 * result + element;
        }
        return result;
    }
}

std::string CheckpointStorageLocationReference::ToString() const
{
    if (encodedReference_ == nullptr) {
        return "(default)";
    } else {
        std::ostringstream oss;
        oss << std::hex << std::setfill('0');
        for (uint8_t byte : *encodedReference_) {
            oss << std::setw(2) << static_cast<int>(byte);
        }
        return oss.str();
    }
}

CheckpointStorageLocationReference *
    CheckpointStorageLocationReference::DEFAULT =
        new CheckpointStorageLocationReference();
