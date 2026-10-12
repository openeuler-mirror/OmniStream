/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#include "MultipleRecordWritersV2.h"
namespace omnistream {

MultipleRecordWritersV2::MultipleRecordWritersV2(std::vector<RecordWriterV2*>& recordWriters)
{
    this->recordWriters.reserve(recordWriters.size());
    for (auto* writer : recordWriters) {
        this->recordWriters.emplace_back(writer);
    }
}

RecordWriterV2* MultipleRecordWritersV2::getRecordWriter(int outputIndex)
{
    // Observer only - ownership stays with this delegate.
    return recordWriters[outputIndex].get();
}

void MultipleRecordWritersV2::close()
{
    LOG_INFO_IMP("MultipleRecordWritersV2 close");
    for (auto& writer : recordWriters) {
        writer->close();
    }
}

void MultipleRecordWritersV2::cancel()
{
    LOG_INFO_IMP("MultipleRecordWritersV2 cancel");
    for (auto& writer : recordWriters) {
        writer->cancel();
    }
}
} // namespace omnistream
