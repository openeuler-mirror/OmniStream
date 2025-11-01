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
#include "CheckpointOptions.h"

CheckpointOptions::CheckpointOptions(
    SnapshotType *checkpointType,
    CheckpointStorageLocationReference *targetLocation,
    AlignmentType alignmentType, long alignedCheckpointTimeout)
    : checkpointType_(checkpointType), targetLocation_(targetLocation),
      alignmentType_(alignmentType), alignedCheckpointTimeout_(alignedCheckpointTimeout)
{
    if (alignmentType == AlignmentType::UNALIGNED && checkpointType->IsSavepoint()) {
        throw std::invalid_argument("Savepoint can't be unaligned");
    }

    if (alignedCheckpointTimeout != NO_ALIGNED_CHECKPOINT_TIME_OUT &&
        alignmentType == AlignmentType::UNALIGNED) {
        throw std::invalid_argument(
            "Unaligned checkpoint can't have timeout (" +
            std::to_string(alignedCheckpointTimeout) + ")");
    }

    if (targetLocation == nullptr) {
        throw std::invalid_argument("targetLocation must not be null");
    }

    if (checkpointType == nullptr) {
        throw std::invalid_argument("checkpointType must not be null");
    }
}

CheckpointOptions::CheckpointOptions(
    SnapshotType *checkpointType,
    CheckpointStorageLocationReference *targetLocation)
    : checkpointType_(checkpointType), targetLocation_(targetLocation),
      alignmentType_(AlignmentType::ALIGNED), alignedCheckpointTimeout_(NO_ALIGNED_CHECKPOINT_TIME_OUT)
{
}

CheckpointOptions *CheckpointOptions::NotExactlyOnce(
    SnapshotType &type, CheckpointStorageLocationReference *location)
{
    return new CheckpointOptions(&type, location, AlignmentType::AT_LEAST_ONCE,
                                 NO_ALIGNED_CHECKPOINT_TIME_OUT);
}

CheckpointOptions *CheckpointOptions::AlignedNoTimeout(
    SnapshotType &type, CheckpointStorageLocationReference *location)
{
    return new CheckpointOptions(&type, location, AlignmentType::ALIGNED,
                                 NO_ALIGNED_CHECKPOINT_TIME_OUT);
}

CheckpointOptions *CheckpointOptions::Unaligned(
    SnapshotType &type, CheckpointStorageLocationReference *location)
{
    if (type.IsSavepoint()) {
        throw std::invalid_argument("Savepoints can not be unaligned");
    }
    return new CheckpointOptions(&type, location, AlignmentType::UNALIGNED,
                                 NO_ALIGNED_CHECKPOINT_TIME_OUT);
}

CheckpointOptions *CheckpointOptions::AlignedWithTimeout(
    SnapshotType &type, CheckpointStorageLocationReference *location,
    long alignedCheckpointTimeout)
{
    if (type.IsSavepoint()) {
        throw std::invalid_argument("Savepoints can not be unaligned");
    }
    return new CheckpointOptions(&type, location, AlignmentType::ALIGNED,
                                 alignedCheckpointTimeout);
}

CheckpointOptions *CheckpointOptions::ForceAligned(
    SnapshotType &type, CheckpointStorageLocationReference *location,
    long alignedCheckpointTimeout)
{
    if (type.IsSavepoint()) {
        throw std::invalid_argument("Savepoints can not be unaligned");
    }
    return new CheckpointOptions(&type, location, AlignmentType::FORCED_ALIGNED,
                                 alignedCheckpointTimeout);
}
CheckpointOptions *CheckpointOptions::FromJson(nlohmann::json &config)
{
    auto alignmentTypeStr = config["alignment"].get<std::string>();
    AlignmentType alignmentType;
    if (alignmentTypeStr == "AT_LEAST_ONCE") {
        alignmentType = AlignmentType::AT_LEAST_ONCE;
    } else if (alignmentTypeStr == "ALIGNED") {
        alignmentType = AlignmentType::ALIGNED;
    } else if (alignmentTypeStr == "UNALIGNED") {
        alignmentType = AlignmentType::UNALIGNED;
    } else if (alignmentTypeStr == "FORCED_ALIGNED") {
        alignmentType = AlignmentType::FORCED_ALIGNED;
    } else {
        throw std::invalid_argument("Unknown alignment type: " + alignmentTypeStr);
    }

    auto alignedCheckpointTimeout = config["alignedCheckpointTimeout"].get<long>();

    CheckpointType *checkpointType;
    if (config["checkpointType"]["name"].get<std::string>() == "Checkpoint") {
        checkpointType = CheckpointType::CHECKPOINT;
    } else if (config["checkpointType"]["name"].get<std::string>() == "FullCheckpoint") {
        checkpointType = CheckpointType::FULL_CHECKPOINT;
    } else {
        throw std::invalid_argument("Unknown checkpoint type");
    }

    CheckpointStorageLocationReference* targetLocation;
    if (config["targetLocation"]["encodedReference"].is_null()) {
        targetLocation = CheckpointStorageLocationReference::GetDefault();
    } else {
        auto encodedReference = new std::vector<uint8_t>(
            config["targetLocation"]["encodedReference"].get<std::vector<uint8_t>>());
        targetLocation = new CheckpointStorageLocationReference(encodedReference);
    }

    return new CheckpointOptions(checkpointType, targetLocation, alignmentType, alignedCheckpointTimeout);
}

CheckpointOptions *CheckpointOptions::ForConfig(
    SnapshotType &checkpointType,
    CheckpointStorageLocationReference *locationReference,
    bool isExactlyOnceMode, bool isUnalignedEnabled,
    long alignedCheckpointTimeout)
{
    if (!isExactlyOnceMode) {
        return NotExactlyOnce(checkpointType, locationReference);
    } else if (checkpointType.IsSavepoint()) {
        return AlignedNoTimeout(checkpointType, locationReference);
    } else if (!isUnalignedEnabled) {
        return AlignedNoTimeout(checkpointType, locationReference);
    } else if (alignedCheckpointTimeout == 0 ||
             alignedCheckpointTimeout == NO_ALIGNED_CHECKPOINT_TIME_OUT) {
        return Unaligned(checkpointType, locationReference);
    } else {
        return AlignedWithTimeout(checkpointType, locationReference,
                                  alignedCheckpointTimeout);
    }
}

long CheckpointOptions::GetAlignedCheckpointTimeout() const
{
    return alignedCheckpointTimeout_;
}

bool CheckpointOptions::NeedsAlignment() const
{
    return IsExactlyOnceMode() &&
           (checkpointType_->IsSavepoint() || !IsUnalignedCheckpoint());
}

bool CheckpointOptions::operator==(const CheckpointOptions &other) const
{
    return *checkpointType_ == (*other.checkpointType_) &&
           *targetLocation_ == (*other.targetLocation_) &&
           alignmentType_ == other.alignmentType_ &&
           alignedCheckpointTimeout_ == other.alignedCheckpointTimeout_;
}

bool CheckpointOptions::operator!=(const CheckpointOptions &other) const
{
    return !(*this == other);
}

std::string CheckpointOptions::ToString() const
{
    std::ostringstream oss;
    oss << "CheckpointOptions{"
        << "checkpointType=" << checkpointType_->ToString()
        << ", targetLocation=" << targetLocation_->ToString()
        << ", alignmentType=" << static_cast<int>(alignmentType_)
        << ", alignedCheckpointTimeout=" << alignedCheckpointTimeout_ << "}";
    return oss.str();
}

CheckpointOptions *CheckpointOptions::ForCheckpointWithDefaultLocation()
{
    return CHECKPOINT_AT_DEFAULT_LOCATION;
}

CheckpointOptions *CheckpointOptions::ToUnaligned() const
{
    if (alignmentType_ != AlignmentType::ALIGNED) {
        throw std::logic_error(
            "toUnaligned() can only be called on ALIGNED checkpoints");
    }
    return Unaligned(*checkpointType_, targetLocation_);
}

CheckpointOptions *CheckpointOptions::CHECKPOINT_AT_DEFAULT_LOCATION =
    new CheckpointOptions(CheckpointType::CHECKPOINT,
                          CheckpointStorageLocationReference::GetDefault());

bool CheckpointOptions::IsTimeoutable() const
{
    if (alignmentType_ == AlignmentType::FORCED_ALIGNED) {
        return false;
    }
    return alignmentType_ == AlignmentType::ALIGNED &&
           (alignedCheckpointTimeout_ > 0 &&
            alignedCheckpointTimeout_ != NO_ALIGNED_CHECKPOINT_TIME_OUT);
}

SnapshotType *CheckpointOptions::GetCheckpointType() const
{
    return checkpointType_;
}

CheckpointStorageLocationReference *CheckpointOptions::GetTargetLocation()
    const
{
    return targetLocation_;
}

CheckpointOptions::AlignmentType CheckpointOptions::GetAlignment() const
{
    return alignmentType_;
}

bool CheckpointOptions::IsExactlyOnceMode() const
{
    return alignmentType_ != AlignmentType::AT_LEAST_ONCE;
}

bool CheckpointOptions::IsUnalignedCheckpoint() const
{
    return alignmentType_ == AlignmentType::UNALIGNED;
}

bool CheckpointOptions::NeedsChannelState() const
{
    return IsUnalignedCheckpoint() || IsTimeoutable();
}

CheckpointOptions *CheckpointOptions::WithUnalignedSupported()
{
    if (alignmentType_ == AlignmentType::FORCED_ALIGNED) {
        if (alignedCheckpointTimeout_ != NO_ALIGNED_CHECKPOINT_TIME_OUT) {
            return AlignedWithTimeout(*checkpointType_, targetLocation_,
                                      alignedCheckpointTimeout_);
        } else {
            return Unaligned(*checkpointType_, targetLocation_);
        }
    }
    return this;
}

CheckpointOptions *CheckpointOptions::WithUnalignedUnsupported()
{
    if (NeedsChannelState()) {
        return ForceAligned(*checkpointType_, targetLocation_,
                            alignedCheckpointTimeout_);
    }
    return this;
}
