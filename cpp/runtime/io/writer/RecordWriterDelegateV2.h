#ifndef FLINK_TNEL_RECORDWRITERDELEGATE_V2_H
#define FLINK_TNEL_RECORDWRITERDELEGATE_V2_H

#include "RecordWriterV2.h"
namespace omnistream{

class RecordWriterDelegateV2 {
public:
    virtual RecordWriterV2*  getRecordWriter(int outputIndex) = 0;

    virtual ~RecordWriterDelegateV2() = default;

    virtual void close() = 0;
};
}

#endif //FLINK_TNEL_RECORDWRITERDELEGATE_H
