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
#include "CheckpointType.h"

CheckpointType *CheckpointType::CHECKPOINT = new CheckpointType(
    "Checkpoint", SnapshotType::SharingFilesStrategy::FORWARD_BACKWARD);

CheckpointType *CheckpointType::FULL_CHECKPOINT = new CheckpointType(
    "FullCheckpoint", SnapshotType::SharingFilesStrategy::FORWARD);

CheckpointType::CheckpointType(
    std::string name, SnapshotType::SharingFilesStrategy sharingFileStrategy)
    : name(name), sharingFilesStrategy_(sharingFileStrategy) {}

bool CheckpointType::operator==(const SnapshotType &other) const
{
    auto castedOther = dynamic_cast<const CheckpointType *>(&other);
    if (!castedOther) {
        return false;
    }
    return name == castedOther->name && sharingFilesStrategy_ == castedOther->sharingFilesStrategy_;
}

std::string CheckpointType::ToString()
{
    return "CheckpointType{name='" + name + "', sharingFilesStrategy=" +
           std::to_string(static_cast<int>(sharingFilesStrategy_)) + "}";
}