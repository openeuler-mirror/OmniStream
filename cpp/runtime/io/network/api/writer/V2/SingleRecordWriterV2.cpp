/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#include "SingleRecordWriterV2.h"

namespace omnistream {
SingleRecordWriterV2::SingleRecordWriterV2(RecordWriterV2* recordWriter) : recordWriter_(recordWriter)
{
}

RecordWriterV2* SingleRecordWriterV2::getRecordWriter(int outputIndex)
{
    // Observer only - ownership stays with this delegate.
    return recordWriter_.get();
}

void SingleRecordWriterV2::close()
{
    LOG_INFO_IMP("SingleRecordWriterV2 close");
    recordWriter_->close();
}

void SingleRecordWriterV2::cancel()
{
    LOG_INFO_IMP("SingleRecordWriterV2 cancel");
    recordWriter_->cancel();
}
} // namespace omnistream
