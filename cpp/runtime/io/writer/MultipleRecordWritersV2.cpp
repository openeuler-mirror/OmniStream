//
// Created by root on 3/25/25.
//

#include "MultipleRecordWritersV2.h"
namespace omnistream {

    MultipleRecordWritersV2::MultipleRecordWritersV2(std::vector<RecordWriterV2*>& recordWriters) : recordWriters(recordWriters) {}


    RecordWriterV2* MultipleRecordWritersV2::getRecordWriter(int outputIndex) {
        return recordWriters[outputIndex];
    }

    void MultipleRecordWritersV2::close()
    {
        LOG_INFO_IMP("MultipleRecordWritersV2 close")
        for (auto* writer : recordWriters) {
            writer->close();
        }
    }

}