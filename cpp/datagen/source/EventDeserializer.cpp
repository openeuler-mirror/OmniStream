/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "EventDeserializer.h"

StreamRecord* BatchEventDeserializer::deserialize(std::unique_ptr<Event> event)
{
    assert(event != nullptr);
    if (collectedCnt == 0) {
        createNewEventBatch();
    }

    reinterpret_cast<Vector<int>*>(vb->GetVectors()[0])->SetValue(collectedCnt, (int) event->getEventType());
    if (event->getEventType() == EventType::PERSON) {
        convertPerson(std::move(event));
        setNullAuction();
        setNullBid();
    } else if (event->getEventType() == EventType::AUCTION) {
        setNullPerson();
        convertAuction(std::move(event));
        setNullBid();
    } else if (event->getEventType() == EventType::BID) {
        setNullPerson();
        setNullAuction();
        convertBid(std::move(event));
    }
    collectedCnt++;
    if (collectedCnt == batchSize) {
        reUseRecord->setValue(vb);
        collectedCnt = 0;
        return reUseRecord;
    }
    return nullptr;
}

void BatchEventDeserializer::createNewEventBatch()
{
    collectedCnt = 0;
    vb = new omnistream::VectorBatch(batchSize);
    std::vector<int> columnTypes = {DataTypeId::OMNI_INT};
    columnTypes.insert(columnTypes.end(), Event::PersonTypes.begin(), Event::PersonTypes.end());
    columnTypes.insert(columnTypes.end(), Event::AuctionTypes.begin(), Event::AuctionTypes.end());
    columnTypes.insert(columnTypes.end(), Event::BidTypes.begin(), Event::BidTypes.end());

    for (auto typeId : columnTypes) {
        BaseVector* vec = nullptr;
        switch (typeId) {
            case DataTypeId::OMNI_INT:
                vec = new Vector<int>(batchSize);
                break;
            case DataTypeId::OMNI_TIMESTAMP:
            case DataTypeId::OMNI_LONG:
                vec = new Vector<int64_t>(batchSize);
                break;
            case DataTypeId::OMNI_VARCHAR:
            case DataTypeId::OMNI_CHAR:
                vec = new Vector<LargeStringContainer<std::string_view>>(batchSize);
                break;
            default:
                std::runtime_error("Not supported datatype");
        }
        vb->Append(vec);
    }
}

void BatchEventDeserializer::convertPerson(std::unique_ptr<Event> event)
{
    static int offset = 1;
    std::unique_ptr<Person> ptr(dynamic_cast<Person*>(event.get()));
    event.release();
    auto vectors = vb->GetVectors();
    // "BIGINT", "STRING", "STRING", "STRING", "STRING", "STRING", "TIMESTAMP", "STRING"
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 0])->SetValue(collectedCnt, ptr->id);
    std::string_view sv(ptr->name.data(), ptr->name.size());
    reinterpret_cast<VarcharVec *>(vectors[offset + 1])->SetValue(collectedCnt, sv);
    reinterpret_cast<VarcharVec *>(vectors[offset + 2])->SetValue(collectedCnt, ptr->emailAddress);
    sv = std::string_view(ptr->creditCard.data(), ptr->creditCard.size());
    reinterpret_cast<VarcharVec *>(vectors[offset + 3])->SetValue(collectedCnt, sv);
    reinterpret_cast<VarcharVec *>(vectors[offset + 4])->SetValue(collectedCnt, ptr->city);
    reinterpret_cast<VarcharVec *>(vectors[offset + 5])->SetValue(collectedCnt, ptr->state);
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 6])->SetValue(collectedCnt, ptr->dateTime);
    reinterpret_cast<VarcharVec *>(vectors[offset + 7])->SetValue(collectedCnt, ptr->extra);
}
void BatchEventDeserializer::setNullPerson()
{
    static int offset = 1;
    auto vectors = vb->GetVectors();
    for (int i = 0; i < 8; i++) {
        vectors[offset + i]->SetNull(collectedCnt);
    }
}
void BatchEventDeserializer::convertAuction(std::unique_ptr<Event> event)
{
    static int offset = 1 + Event::PersonTypes.size();
    std::unique_ptr<Auction> ptr(dynamic_cast<Auction*>(event.get()));
    event.release();
    auto vectors = vb->GetVectors();
    // "BIGINT", "STRING", "STRING", "BIGINT", "BIGINT", "TIMESTAMP",  "TIMESTAMP", "BIGINT", "BIGINT", "STRING"
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 0])->SetValue(collectedCnt, ptr->id);
    reinterpret_cast<VarcharVec *>(vectors[offset + 1])->SetValue(collectedCnt, ptr->itemName);
    reinterpret_cast<VarcharVec *>(vectors[offset + 2])->SetValue(collectedCnt, ptr->description);
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 3])->SetValue(collectedCnt, ptr->initialBid);
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 4])->SetValue(collectedCnt, ptr->reserve);
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 5])->SetValue(collectedCnt, ptr->dateTime);
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 6])->SetValue(collectedCnt, ptr->expires);
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 7])->SetValue(collectedCnt, ptr->seller);
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 8])->SetValue(collectedCnt, ptr->category);
    reinterpret_cast<VarcharVec *>(vectors[offset + 9])->SetValue(collectedCnt, ptr->extra);
}
void BatchEventDeserializer::setNullAuction()
{
    static int offset = 1 + Event::PersonTypes.size();
    auto vectors = vb->GetVectors();
    for (int i = 0; i < 10; i++) {
        vectors[offset + i]->SetNull(collectedCnt);
    }
}
void BatchEventDeserializer::convertBid(std::unique_ptr<Event> event)
{
    static int offset = 1 + Event::PersonTypes.size() + Event::AuctionTypes.size();
    //"BIGINT", "BIGINT", "BIGINT", "STRING", "STRING", "TIMESTAMP", "STRING"
    std::unique_ptr<Bid> ptr(dynamic_cast<Bid*>(event.get()));
    event.release();
    auto vectors = vb->GetVectors();
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 0])->SetValue(collectedCnt, ptr->auction);
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 1])->SetValue(collectedCnt, ptr->bidder);
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 2])->SetValue(collectedCnt, ptr->price);
    reinterpret_cast<VarcharVec *>(vectors[offset + 3])->SetValue(collectedCnt, ptr->channel);
    reinterpret_cast<VarcharVec *>(vectors[offset + 4])->SetValue(collectedCnt, ptr->url);
    reinterpret_cast<Vector<int64_t>* >(vectors[offset + 5])->SetValue(collectedCnt, ptr->dateTime);
    reinterpret_cast<VarcharVec *>(vectors[offset + 6])->SetValue(collectedCnt, ptr->extra);
}
void BatchEventDeserializer::setNullBid()
{
    static int offset = 1 + Event::PersonTypes.size() + Event::AuctionTypes.size();
    auto vectors = vb->GetVectors();
    for (int i = 0; i < 7; i++) {
        vectors[offset + i]->SetNull(collectedCnt);
    }
}