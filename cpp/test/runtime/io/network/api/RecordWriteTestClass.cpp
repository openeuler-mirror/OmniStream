#include "RecordWriteTestClass.h"


RecordWriterV2TestClass::RecordWriterV2TestClass(
    std::shared_ptr<omnistream::ResultPartitionWriter> writer,
    long timeout,
    const std::string& taskName,int tasktype)
    : omnistream::RecordWriterV2(writer, timeout, taskName,tasktype) {}
