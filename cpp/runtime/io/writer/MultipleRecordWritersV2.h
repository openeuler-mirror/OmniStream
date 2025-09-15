#ifndef MULTIPLERECORDWRITERSV2_H
#define MULTIPLERECORDWRITERSV2_H
#include "RecordWriterDelegateV2.h"

namespace omnistream {
    class MultipleRecordWritersV2 : public RecordWriterDelegateV2 {
    public:
        explicit MultipleRecordWritersV2(std::vector<RecordWriterV2*>& recordWriters);

        RecordWriterV2* getRecordWriter(int outputIndex) override;

        ~MultipleRecordWritersV2() override = default;

        void close() override;

    private:
        std::vector<RecordWriterV2*> recordWriters;

    };
}

#endif //MULTIPLERECORDWRITERSV2_H
