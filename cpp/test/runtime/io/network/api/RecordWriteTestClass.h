#ifndef RECORDWRITERTESTCLASS_H
#define RECORDWRITERTESTCLASS_H
#include "runtime/io/network/api/writer/V2/RecordWriterV2.h"


class RecordWriterV2TestClass : public omnistream::RecordWriterV2 {
public:
    RecordWriterV2TestClass(
        std::shared_ptr<omnistream::ResultPartitionWriter> writer, // Raw pointer
        long timeout,
        const std::string& taskName,int tasktype);


    ~RecordWriterV2TestClass() = default;


    void emit(StreamRecord * record) override {

    };

    void broadcastEmit(Watermark *watermark) override {

    };
protected:
    void emit(StreamRecord *record, int targetSubpartition) override {
    };
};



#endif //RECORDWRITERTESTCLASS_H
