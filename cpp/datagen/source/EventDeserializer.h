/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_EVENTDESERIALIZER_H
#define OMNISTREAM_EVENTDESERIALIZER_H
#include <optional>
#include "table/vectorbatch/VectorBatch.h"
#include "../nexmark/model/Event.h"
#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "core/streamrecord/StreamRecord.h"
using namespace omniruntime::vec;
using namespace omniruntime::type;

class EventDeserializer {
public:
    EventDeserializer() = default;
    virtual ~EventDeserializer() = default;
    virtual StreamRecord* deserialize(std::unique_ptr<Event> event) = 0;
};

class BatchEventDeserializer : public EventDeserializer {
public:
    using VarcharVec = Vector<LargeStringContainer<std::string_view>>;
    explicit BatchEventDeserializer(int batchSize) : batchSize(batchSize)
    {
        reUseRecord = new StreamRecord();
    }
    ~BatchEventDeserializer() override = default;
    // Deserialize an in coming event into batch
    StreamRecord* deserialize(std::unique_ptr<Event> event) override;

private:
    void createNewEventBatch();
    void convertPerson(std::unique_ptr<Event> event);
    void convertAuction(std::unique_ptr<Event> event);
    void convertBid(std::unique_ptr<Event> event);

    void setNullPerson();
    void setNullAuction();
    void setNullBid();

    int batchSize;
    omnistream::VectorBatch* vb = nullptr;
    int collectedCnt = 0;
    StreamRecord* reUseRecord{};
};


#endif // OMNISTREAM_EVENTDESERIALIZER_H
