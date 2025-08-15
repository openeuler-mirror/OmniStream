//
// Created by c00572813 on 2025/4/14.
//
#include <gtest/gtest.h>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "typeutils/TypeSerializer.h"
#include "typeutils/BinaryRowDataSerializer.h"
#include "typeutils/LongSerializer.h"
#include "io/DataInputDeserializer.h"
#include "emhash7.hpp"
#include <filesystem>
#include "table/runtime/operators/window/TimeWindow.h"
#include "table/typeutils/BinaryRowDataListSerializer.h"
#include "state/rocksdb/RocksdbListState.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/rocksdb/RocksdbStateTable.h"
#include "state/VoidNamespaceSerializer.h"
#include "runtime/state/rocksdb/RocksdbValueState.h"
#include "core/api/common/state/ValueStateDescriptor.h"
#include "state/rocksdb/RocksDbStringAppendOperator.h"

namespace fs = std::filesystem;
using namespace ROCKSDB_NAMESPACE;

std::string kDBPath = "/tmp/rocksdb_ut/";

std::string getRocksDbPath() {
    auto now = std::chrono::system_clock::now();
    auto nano_seconds = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
    return kDBPath + std::to_string(nano_seconds) + "_";
}

TEST(RocksDBTest, BasicTest) {
    // restore rocksdb tmp Directory
    fs::remove_all(fs::path(kDBPath));
    fs::create_directories(fs::path(kDBPath));

    DB* db;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &db);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    // Put key-value
    s = db->Put(WriteOptions(), "key1", "value222");
    assert(s.ok());
    std::string value;
    // get value
    s = db->Get(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value222");
    db->Close();
}

TEST(RocksDBTest, BinaryRowDataTest) {
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    TypeSerializer *vSerializer = new BinaryRowDataSerializer(1);
    DataOutputSerializer valueOutputSerializer;
    OutputBufferStatus valueOutputBufferStatus;
    valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

    BinaryRowData* tmpS = BinaryRowData::createBinaryRowDataWithMem(3);
    tmpS->setLong(0, 100);
    tmpS->setLong(1, 200);
    tmpS->setLong(2, 300);

//    if constexpr (std::is_pointer_v<BinaryRowData>) {
    vSerializer->serialize(tmpS, valueOutputSerializer);
//    } else {
//        vSerializer->serialize(&tmpS, valueOutputSerializer);
//    }

    ROCKSDB_NAMESPACE::Slice sliceKey("key1");
    ROCKSDB_NAMESPACE::Slice sliceValue(reinterpret_cast<const char *>(valueOutputSerializer.getData()),
                                        valueOutputSerializer.length());
    rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), sliceKey, sliceValue);

    std::string valueInTable;
    rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), sliceKey, &valueInTable);

    DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(valueInTable.data()), valueInTable.length(), 0);
    BinaryRowData* resPtr = reinterpret_cast<BinaryRowData*>(vSerializer->deserialize(serializedData));
//    ASSERT_EQ(100, *resPtr->getLong(0));
    for(int i = 0; i < 3; i++) {
        std::cout << *resPtr->getLong(i) << "|";
    }
    rocksDb->Close();
}

TEST(RocksDBTest, BinaryRowDataStringTest) {
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    TypeSerializer *vSerializer = new BinaryRowDataSerializer(1);
    DataOutputSerializer valueOutputSerializer;
    OutputBufferStatus valueOutputBufferStatus;
    valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

    int arity = 4;
    std::string_view sv0 = "hello";
    std::string_view sv1 = "hellohello_worldworld_abcdefg";
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(arity);
    row->setLong(0, 42);
    row->setStringView(1, sv0);
    row->setInt(2, 42);
    row->setStringView(3, sv1);

    EXPECT_EQ(*row->getLong(0), 42);
    EXPECT_EQ(row->getStringView(1), sv0);
    EXPECT_EQ(*row->getInt(2), 42);
    EXPECT_EQ(row->getStringView(3), sv1);

    vSerializer->serialize(row, valueOutputSerializer);

    ROCKSDB_NAMESPACE::Slice sliceKey("key1");
    ROCKSDB_NAMESPACE::Slice sliceValue(reinterpret_cast<const char *>(valueOutputSerializer.getData()),
                                        valueOutputSerializer.length());
    rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), sliceKey, sliceValue);

    std::string valueInTable;
    rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), sliceKey, &valueInTable);

    DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(valueInTable.data()), valueInTable.length(), 0);
    BinaryRowData* resPtr = reinterpret_cast<BinaryRowData*>(vSerializer->deserialize(serializedData));
    EXPECT_EQ(*resPtr->getLong(0), 42);
    EXPECT_EQ(resPtr->getStringView(1), sv0);
    EXPECT_EQ(*resPtr->getInt(2), 42);
    EXPECT_EQ(resPtr->getStringView(3), sv1);
    rocksDb->Close();
}

TEST(RocksDBTest, BinaryRowDataAndLongTest) {
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    TypeSerializer *keySerializer = new BinaryRowDataSerializer(1);
    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus keyOutputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&keyOutputBufferStatus);

    BinaryRowData* key = BinaryRowData::createBinaryRowDataWithMem(2);
    key->setLong(0, 100);
    key->setLong(1, 101);


    keySerializer->serialize(key, keyOutputSerializer);
    ROCKSDB_NAMESPACE::Slice sliceKey(reinterpret_cast<const char *>(keyOutputSerializer.getData()),
                                      keyOutputSerializer.length());


    long value = 100;
    TypeSerializer *vSerializer = new LongSerializer();
    DataOutputSerializer valueOutputSerializer;
    OutputBufferStatus valueOutputBufferStatus;
    valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
    vSerializer->serialize(&value, valueOutputSerializer);
    ROCKSDB_NAMESPACE::Slice sliceValue(reinterpret_cast<const char *>(valueOutputSerializer.getData()),
                                        valueOutputSerializer.length());
    rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), sliceKey, sliceValue);


    BinaryRowData* key_copy = BinaryRowData::createBinaryRowDataWithMem(2);
    key_copy->setLong(0, 100);
    key_copy->setLong(1, 101);
    TypeSerializer *keySerializer_copy = new BinaryRowDataSerializer(2);
    DataOutputSerializer keyOutputSerializer_copy;
    OutputBufferStatus keyOutputBufferStatus_copy;
    keyOutputSerializer_copy.setBackendBuffer(&keyOutputBufferStatus_copy);
    keySerializer_copy->serialize(key_copy, keyOutputSerializer_copy);

    ROCKSDB_NAMESPACE::Slice slicekey_copy(reinterpret_cast<const char *>(keyOutputSerializer_copy.getData()),
                                           keyOutputSerializer_copy.length());
    std::cout << "keyOutputSerializer_copy.length() is " << keyOutputSerializer_copy.length() << std::endl;

    std::string valueInTable;
    rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), slicekey_copy, &valueInTable);
    std::cout << "valueInTable length is " << valueInTable.length() << std::endl;

    DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(valueInTable.data()), valueInTable.length(), 0);
    int res = serializedData.readLong();
    ASSERT_EQ(100, res);
    std::cout << "success" << std::endl;


    BinaryRowData* key_copy2 = BinaryRowData::createBinaryRowDataWithMem(2);
    key_copy2->setLong(0, 100);
    key_copy2->setLong(1, 102);
    TypeSerializer *keySerializer_copy2 = new BinaryRowDataSerializer(1);
    DataOutputSerializer keyOutputSerializer_copy2;
    OutputBufferStatus keyOutputBufferStatus_copy2;
    keyOutputSerializer_copy2.setBackendBuffer(&keyOutputBufferStatus_copy2);
    keySerializer_copy2->serialize(key_copy2, keyOutputSerializer_copy2);

    ROCKSDB_NAMESPACE::Slice slicekey_copy2(reinterpret_cast<const char *>(keyOutputSerializer_copy2.getData()),
                                            keyOutputSerializer_copy2.length());
    std::cout << "keyOutputSerializer_copy2.length() is " << keyOutputSerializer_copy2.length() << std::endl;

    std::string valueInTable2;
    auto status = rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), slicekey_copy2, &valueInTable2);

    std::cout << "valueInTable2 length is " << valueInTable2.length() << std::endl;

    ASSERT_EQ(true, s.ok());
    std::cout << "success2" << std::endl;
    rocksDb->Close();
}

TEST(RocksDBTest, GetKeyTest) {
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    TypeSerializer *keySerializer = new BinaryRowDataSerializer(1);
    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus keyOutputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&keyOutputBufferStatus);

    BinaryRowData* key = BinaryRowData::createBinaryRowDataWithMem(2);
    key->setLong(0, 100);
    key->setLong(1, 101);


    keySerializer->serialize(key, keyOutputSerializer);
    ROCKSDB_NAMESPACE::Slice sliceKey(reinterpret_cast<const char *>(keyOutputSerializer.getData()),
                                      keyOutputSerializer.length());

    std::string valueInTable;
    s = rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), sliceKey, &valueInTable);
    std::cout << (int)s.code() << std::endl;
//    ASSERT_EQ(true, s.IsNotFound());
//    ASSERT_EQ(0, valueInTable.length());
//    std::cout << "valueInTable length is: " << valueInTable.length() << ", valueInTable is : " << valueInTable << std::endl;
    rocksDb->Close();
}

TEST(RocksDBTest, EntriesTest) {
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    // put BinaryRowData(100, 101), 100
    TypeSerializer *keySerializer = new BinaryRowDataSerializer(1);
    DataOutputSerializer keyOutputSerializer;
    OutputBufferStatus keyOutputBufferStatus;
    keyOutputSerializer.setBackendBuffer(&keyOutputBufferStatus);

    BinaryRowData* key = BinaryRowData::createBinaryRowDataWithMem(2);
    key->setLong(0, 100);
    key->setLong(1, 101);

    keySerializer->serialize(key, keyOutputSerializer);
    ROCKSDB_NAMESPACE::Slice sliceKey(reinterpret_cast<const char *>(keyOutputSerializer.getData()),
                                      keyOutputSerializer.length());

    long value = 100;
    TypeSerializer *vSerializer = new LongSerializer();
    DataOutputSerializer valueOutputSerializer;
    OutputBufferStatus valueOutputBufferStatus;
    valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);
    vSerializer->serialize(&value, valueOutputSerializer);
    ROCKSDB_NAMESPACE::Slice sliceValue(reinterpret_cast<const char *>(valueOutputSerializer.getData()),
                                        valueOutputSerializer.length());
//    rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), sliceKey, sliceValue);

    BinaryRowDataSerializer* userKeySerializer = new BinaryRowDataSerializer(1);
    LongSerializer* valueSerializer = new LongSerializer();

    // get entries data
    emhash7::HashMap<BinaryRowData*, long> resultMap;

    ROCKSDB_NAMESPACE::Iterator* iterator = rocksDb->NewIterator(ROCKSDB_NAMESPACE::ReadOptions());
    iterator->Seek(sliceKey);
    // 遍历以指定前缀开头的键值对
    for (; iterator->Valid() && iterator->key().starts_with(sliceKey); iterator->Next()) {
        ROCKSDB_NAMESPACE::Slice key = iterator->key();
        ROCKSDB_NAMESPACE::Slice value = iterator->value();
        BinaryRowData* entryKey;
        long entryValue;

        DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(key.data()), key.size(), 0);
        void *resPtr = userKeySerializer->deserialize(serializedData);
        entryKey = (BinaryRowData*)resPtr;

        serializedData = DataInputDeserializer(reinterpret_cast<const uint8_t *>(value.data()), value.size(), 0);
        entryValue = serializedData.readLong();
        resultMap.emplace(entryKey, entryValue);
    }
    for (const auto& pair : resultMap) {
        std::cout << "Key: " << std::endl;
        pair.first->printRow();
        std::cout << ", Value: " << pair.second << std::endl;
    }
    std::cout << "success!" << std::endl;
    rocksDb->Close();
}

TEST(RocksDBTest, TimeWindowTypeTest) {
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    TypeSerializer *vSerializer = new TimeWindow::Serializer();
    DataOutputSerializer valueOutputSerializer;
    OutputBufferStatus valueOutputBufferStatus;
    valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

    TimeWindow timeWindow(1000, 2000);

    vSerializer->serialize(&timeWindow, valueOutputSerializer);

    ROCKSDB_NAMESPACE::Slice sliceKey("key1");
    ROCKSDB_NAMESPACE::Slice sliceValue(reinterpret_cast<const char *>(valueOutputSerializer.getData()),
                                        valueOutputSerializer.length());
    rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), sliceKey, sliceValue);

    std::string valueInTable;
    rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), sliceKey, &valueInTable);

    DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(valueInTable.data()), valueInTable.length(), 0);
    TimeWindow* resPtr = reinterpret_cast<TimeWindow*>(vSerializer->deserialize(serializedData));
    ASSERT_EQ(1000, resPtr->start);
    ASSERT_EQ(2000, resPtr->end);
    rocksDb->Close();
}

TEST(RocksDBTest, BinaryRowDataListTest) {
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());

    TypeSerializer *vSerializer = new BinaryRowDataListSerializer();
    DataOutputSerializer valueOutputSerializer;
    OutputBufferStatus valueOutputBufferStatus;
    valueOutputSerializer.setBackendBuffer(&valueOutputBufferStatus);

    std::vector<RowData*>* rowDataList = new std::vector<RowData*>();
    BinaryRowData* binaryRowData1 = BinaryRowData::createBinaryRowDataWithMem(2);
    binaryRowData1->setLong(0, 10);
    binaryRowData1->setLong(1, 20);
    BinaryRowData* binaryRowData2 = BinaryRowData::createBinaryRowDataWithMem(2);
    binaryRowData2->setLong(0, 30);
    binaryRowData2->setLong(1, 40);
    rowDataList->push_back(binaryRowData1);
    rowDataList->push_back(binaryRowData2);
    ASSERT_EQ(10, *rowDataList->at(0)->getLong(0));
    ASSERT_EQ(20, *rowDataList->at(0)->getLong(1));
    ASSERT_EQ(30, *rowDataList->at(1)->getLong(0));
    ASSERT_EQ(40, *rowDataList->at(1)->getLong(1));

    vSerializer->serialize(rowDataList, valueOutputSerializer);

    ROCKSDB_NAMESPACE::Slice sliceKey("key1");
    ROCKSDB_NAMESPACE::Slice sliceValue(reinterpret_cast<const char *>(valueOutputSerializer.getData()),
                                        valueOutputSerializer.length());
    rocksDb->Put(ROCKSDB_NAMESPACE::WriteOptions(), sliceKey, sliceValue);

    std::string valueInTable;
    rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), sliceKey, &valueInTable);

    DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(valueInTable.data()), valueInTable.length(), 0);
    std::vector<RowData*>* resPtr = reinterpret_cast<std::vector<RowData*>*>(vSerializer->deserialize(serializedData));
    ASSERT_EQ(10, *resPtr->at(0)->getLong(0));
    ASSERT_EQ(20, *resPtr->at(0)->getLong(1));
    ASSERT_EQ(30, *resPtr->at(1)->getLong(0));
    ASSERT_EQ(40, *resPtr->at(1)->getLong(1));
    rocksDb->Close();
}

TEST(RocksDBTest, MergeTest) {
    DB* rocksDb;
    Options options;
    options.merge_operator.reset(new RocksDbStringAppendOperator(','));
    options.create_if_missing = true;

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());
    std::string key = "key";
    std::string valueInTable;

    Status status = rocksDb->Merge(ROCKSDB_NAMESPACE::WriteOptions(), key, "1");
    status = rocksDb->Merge(ROCKSDB_NAMESPACE::WriteOptions(), key, "2");
    status = rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), key, &valueInTable);
    ASSERT_EQ("1,2", valueInTable);

    LongSerializer *serializer = LongSerializer::INSTANCE;
    int64_t currentKey = 100;
    // serializer long data
    DataOutputSerializer outputSerializer;
    OutputBufferStatus outputBufferStatus;
    outputSerializer.setBackendBuffer(&outputBufferStatus);
    serializer->serialize(&currentKey, outputSerializer);
    ROCKSDB_NAMESPACE::Slice keySlice(reinterpret_cast<const char *>(outputSerializer.getData()), outputSerializer.length());
    ROCKSDB_NAMESPACE::Slice valueSlice(reinterpret_cast<const char *>(outputSerializer.getData()), outputSerializer.length());
    status = rocksDb->Merge(ROCKSDB_NAMESPACE::WriteOptions(), keySlice, valueSlice);
    status = rocksDb->Merge(ROCKSDB_NAMESPACE::WriteOptions(), keySlice, valueSlice);
    status = rocksDb->Get(ROCKSDB_NAMESPACE::ReadOptions(), keySlice, &valueInTable);

    DataInputDeserializer serializedData(reinterpret_cast<const uint8_t *>(valueInTable.data()),
                                         valueInTable.length(), 0);
    auto*result = new std::vector<int64_t>();
    while (serializedData.Available() > 0) {
        void *resPtr = serializer->deserialize(serializedData);
        result->push_back(*(int64_t *)resPtr);
        if (serializedData.Available() > 0) {
            serializedData.readByte();
        }
    }
    for (const auto &item: *result) {
        std::cout << "Rocks merge result: " << item << std::endl;
    }
    rocksDb->Close();
}

TEST(RocksDBTest, ListStateVectorBatchTest) {
    // Initialize serializers
    LongSerializer *serializer = LongSerializer::INSTANCE;
    // Initialize the InternalKeyContext
    auto *context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 1);
    BinaryRowData* binaryRowData = BinaryRowData::createBinaryRowDataWithMem(1);
    binaryRowData->setLong(0, 100);
    context->setCurrentKey(binaryRowData);
    context->setCurrentKeyGroupIndex(1);
    // Initialize RegisteredKeyValueStateBackendMetaInfo
    auto *metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", serializer, serializer);
    RocksdbStateTable<RowData*, int64_t, int64_t> rocksdbStateTable(context, metaInfo, new BinaryRowDataSerializer(1));
    // Create HeapListState
    auto* rocksdbListState = new RocksdbListState(&rocksdbStateTable, serializer, new LongSerializer());
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;
    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());
    rocksdbListState->createTable(rocksDb, "ListStateVectorBatchTest");
    rocksdbListState->setCurrentNamespace(1000);

    // add VectorBatch
    auto vbatchLeft = new omnistream::VectorBatch(2);
    auto vKeyLeft = new omniruntime::vec::Vector<int64_t>(2);
    vKeyLeft->SetValue(0, 0);
    vKeyLeft->SetValue(1, 1);
    vbatchLeft->Append(vKeyLeft);
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(2);
    vWindowEndTimeLeft->SetValue(0, 1000);
    vWindowEndTimeLeft->SetValue(1, 2000);
    vbatchLeft->Append(vWindowEndTimeLeft);
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(2);
    vValLeft->SetValue(0, 12);
    vValLeft->SetValue(1, 24);
    vbatchLeft->Append(vValLeft);
    rocksdbListState->addVectorBatch(vbatchLeft);

    // verify getVectorBatch
    ASSERT_EQ(0, rocksdbListState->getVectorBatch(0)->GetValueAt<int64_t>(0, 0));
    ASSERT_EQ(1000, rocksdbListState->getVectorBatch(0)->GetValueAt<int64_t>(1, 0));
    ASSERT_EQ(12, rocksdbListState->getVectorBatch(0)->GetValueAt<int64_t>(2, 0));
    ASSERT_EQ(1, rocksdbListState->getVectorBatch(0)->GetValueAt<int64_t>(0, 1));
    ASSERT_EQ(2000, rocksdbListState->getVectorBatch(0)->GetValueAt<int64_t>(1, 1));
    ASSERT_EQ(24, rocksdbListState->getVectorBatch(0)->GetValueAt<int64_t>(2, 1));
    rocksDb->Close();
}

TEST(RocksDBTest, ValueStateTest) {
    // Initialize serializers
    VoidNamespaceSerializer *voidNamespaceSerializer = new VoidNamespaceSerializer();
    BinaryRowDataSerializer *binaryRowDataSerializer = new BinaryRowDataSerializer(1);
    // Initialize the InternalKeyContext
    InternalKeyContextImpl<RowData*> *context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 1);
    BinaryRowData* keyRowData = BinaryRowData::createBinaryRowDataWithMem(1);
    keyRowData->setLong(0, 100);
    context->setCurrentKey(keyRowData);
    context->setCurrentKeyGroupIndex(1);
    // Initialize RegisteredKeyValueStateBackendMetaInfo
    RegisteredKeyValueStateBackendMetaInfo *metaInfo =
            new RegisteredKeyValueStateBackendMetaInfo("metaInfo", voidNamespaceSerializer, binaryRowDataSerializer);
    RocksdbStateTable<RowData*, VoidNamespace, RowData*> rocksdbStateTable(context, metaInfo, binaryRowDataSerializer);

    // Create HeapListState
    RocksdbValueState<RowData*, VoidNamespace, RowData*>* rocksdbValueState = RocksdbValueState<RowData*, VoidNamespace, RowData*>::create(
            new ValueStateDescriptor("ValueStateTest", binaryRowDataSerializer), &rocksdbStateTable, binaryRowDataSerializer);

    DB* rocksDb;
    Options options;
    options.create_if_missing = true;
    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());
    rocksdbValueState->createTable(rocksDb, "ValueStateTest");
    // Test: Add a BinaryRowData to the ValueState
    BinaryRowData* value = BinaryRowData::createBinaryRowDataWithMem(3);
    value->setLong(0, 100);
    value->setLong(1, 200);
    value->setLong(2, 300);
    rocksdbValueState->update(value);
    BinaryRowData* result = dynamic_cast<BinaryRowData *>(rocksdbValueState->value());

    EXPECT_EQ(*result->getLong(0), 100);
    EXPECT_EQ(*result->getLong(1), 200);
    EXPECT_EQ(*result->getLong(2), 300);

    // Test: Update BinaryRowData to the ValueState
    value->setLong(0, 200);
    result = dynamic_cast<BinaryRowData *>(rocksdbValueState->value());
    EXPECT_EQ(*result->getLong(0), 100);
    EXPECT_EQ(*result->getLong(1), 200);
    EXPECT_EQ(*result->getLong(2), 300);
    rocksdbValueState->update(value);
    result = dynamic_cast<BinaryRowData *>(rocksdbValueState->value());

    EXPECT_EQ(*result->getLong(0), 200);
    EXPECT_EQ(*result->getLong(1), 200);
    EXPECT_EQ(*result->getLong(2), 300);

    // Test: new BinaryRowData, get empty value
    context->setCurrentKey(BinaryRowData::createBinaryRowDataWithMem(1));
    result = dynamic_cast<BinaryRowData *>(rocksdbValueState->value());
    ASSERT_EQ(nullptr, result);

    // Test: clear ValueState
    context->setCurrentKey(keyRowData);
    result = dynamic_cast<BinaryRowData *>(rocksdbValueState->value());

    EXPECT_EQ(*result->getLong(0), 200);
    EXPECT_EQ(*result->getLong(1), 200);
    EXPECT_EQ(*result->getLong(2), 300);
    rocksdbValueState->clear();
    result = dynamic_cast<BinaryRowData *>(rocksdbValueState->value());
    ASSERT_EQ(nullptr, result);
    rocksDb->Close();
}

TEST(RocksDBTest, ListStateTest) {
    // Initialize serializers
    LongSerializer *serializer = LongSerializer::INSTANCE;
    // Initialize the InternalKeyContext
    auto *context = new InternalKeyContextImpl<RowData*>(new KeyGroupRange(0, 1), 1);
    BinaryRowData* binaryRowData = BinaryRowData::createBinaryRowDataWithMem(1);
    binaryRowData->setLong(0, 100);
    context->setCurrentKey(binaryRowData);
    context->setCurrentKeyGroupIndex(1);
    // Initialize RegisteredKeyValueStateBackendMetaInfo
    auto *metaInfo = new RegisteredKeyValueStateBackendMetaInfo("metaInfo", serializer, serializer);
    RocksdbStateTable<RowData*, int64_t, int64_t> rocksdbStateTable(context, metaInfo, new BinaryRowDataSerializer(1));
    // Create HeapListState
    auto* rocksdbListState = new RocksdbListState(&rocksdbStateTable, serializer, new LongSerializer());
    DB* rocksDb;
    Options options;
    options.create_if_missing = true;
    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    Status s = DB::Open(options, getRocksDbPath() + test_info->name(), &rocksDb);
    std::cout << (int)s.code() << std::endl;
    assert(s.ok());
    rocksdbListState->createTable(rocksDb, "ListStateTest");
    rocksdbListState->setCurrentNamespace(1000);
    // Test: Add a value to the list
    rocksdbListState->add(10);
    auto list = rocksdbListState->get();

    // Verify: Check if the value is in the list
    ASSERT_TRUE(list != nullptr);
    EXPECT_EQ(list->size(), 1);
    EXPECT_EQ((*list)[0], 10);

    // Test: Add another value to the list
    rocksdbListState->add(20);
    list = rocksdbListState->get();

    // Verify: Check the updated list
    ASSERT_TRUE(list != nullptr);
    EXPECT_EQ(list->size(), 2);
    EXPECT_EQ((*list)[0], 10);
    EXPECT_EQ((*list)[1], 20);

    // Test: Clear the state
    rocksdbListState->clear();
    list = rocksdbListState->get();

    // Verify: List should now be empty
    ASSERT_TRUE(list == nullptr);
    rocksDb->Close();
}
