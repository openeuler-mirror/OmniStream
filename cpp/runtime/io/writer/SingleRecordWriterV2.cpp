#include "SingleRecordWriterV2.h"

namespace omnistream
{
    SingleRecordWriterV2::SingleRecordWriterV2(RecordWriterV2* recordWriter) : recordWriter_(recordWriter) {}

    RecordWriterV2* SingleRecordWriterV2::getRecordWriter(int outputIndex) {
        return recordWriter_;
    }

    void SingleRecordWriterV2::close()
    {
        LOG_INFO_IMP("SingleRecordWriterV2 close")
        recordWriter_->close();
    }
}
