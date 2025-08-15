//
// Created by root on 3/26/25.
//

#ifndef NONRECORDWRITERV2_H
#define NONRECORDWRITERV2_H

#include "RecordWriterDelegateV2.h"

namespace omnistream {
    class NonRecordWriterV2 : public RecordWriterDelegateV2 {

    public:
        NonRecordWriterV2() = default;

        RecordWriterV2* getRecordWriter(int outputIndex) override {
            throw std::invalid_argument("NonRecordWriterV2 unsupport");
        }

        void close() override {}


    };
}


#endif //NONRECORDWRITERV2_H
