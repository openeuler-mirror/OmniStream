#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <array>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "core/utils/ByteView.h"
#include "runtime/state/CompositeKeySerializationUtils.h"
#include "runtime/state/RegisteredStateMetaInfoBase.h"
#include "runtime/state/RocksDBFullSnapshotResources.h"
#include "runtime/state/RocksDBVectorBatchStateAccessor.h"
#include "runtime/state/VectorBatchStateAccessor.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/rocksdb/util/ResourceGuard.h"
#include "table/data/RowData.h"
#include "table/data/RowKind.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "table/utils/VectorBatchSerializationUtils.h"

namespace fs = std::filesystem;

namespace {

constexpr const char* kDbRootPath = "/tmp/rocksdb_vb_accessor_ut/";
constexpr const char* kVectorBatchColumnFamily = "vb_test_cf";
constexpr int64_t kBatchId = 42;
constexpr int32_t kNumberOfKeyGroups = 512;
constexpr int32_t kKeyGroup = 257;
constexpr int32_t kSerializedVectorBatchBufferBytes = 64 * 1024;

std::string makeRocksDbPath()
{
    auto now = std::chrono::system_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    const ::testing::TestInfo* testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
    return std::string(kDbRootPath) + std::to_string(ns) + "_" + testInfo->name();
}

std::vector<int8_t> serializeVectorBatch(omnistream::VectorBatch* batch)
{
    int32_t batchSize = omnistream::VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(batch);
    if (batchSize <= 0 || batchSize > kSerializedVectorBatchBufferBytes) {
        throw std::runtime_error("serializeVectorBatch calculated invalid batch size.");
    }
    std::array<uint8_t, kSerializedVectorBatchBufferBytes> buffer{};
    uint8_t* writeCursor = buffer.data();
    omnistream::SerializedBatchInfo serialized =
        omnistream::VectorBatchSerializationUtils::serializeVectorBatch(batch, batchSize, writeCursor);

    return std::vector<int8_t>(
        reinterpret_cast<int8_t*>(serialized.buffer), reinterpret_cast<int8_t*>(serialized.buffer + serialized.size));
}

std::vector<int8_t> serializeVectorBatch(int32_t rowCount)
{
    auto batch = std::make_unique<omnistream::VectorBatch>(rowCount);
    for (int32_t row = 0; row < rowCount; ++row) {
        batch->setTimestamp(row, 1000 + row);
        batch->setRowKind(row, RowKind::INSERT);
    }
    return serializeVectorBatch(batch.get());
}

std::vector<int8_t> makeVbKey(int64_t batchId, int32_t keyGroupPrefixBytes, int32_t keyGroup)
{
    OutputBufferStatus outputBufferStatus;
    DataOutputSerializer outputSerializer;
    outputSerializer.setBackendBuffer(&outputBufferStatus);

    CompositeKeySerializationUtils::writeKeyGroup(keyGroup, keyGroupPrefixBytes, outputSerializer);
    LongSerializer longSerializer;
    longSerializer.serialize(&batchId, outputSerializer);

    return std::vector<int8_t>(
        reinterpret_cast<int8_t*>(outputSerializer.getData()),
        reinterpret_cast<int8_t*>(outputSerializer.getData() + outputSerializer.getPosition()));
}

VectorBatchAccessorOptions optionsWithCacheBytes(size_t maxDecodedBatchCacheBytes)
{
    VectorBatchAccessorOptions options;
    options.maxDecodedBatchCacheBytes = maxDecodedBatchCacheBytes;
    return options;
}

class TestRegisteredStateMetaInfo : public RegisteredStateMetaInfoBase {
public:
    explicit TestRegisteredStateMetaInfo(const std::string& name) : RegisteredStateMetaInfoBase(name)
    {
    }

    std::shared_ptr<StateMetaInfoSnapshot> snapshot() override
    {
        return std::make_shared<StateMetaInfoSnapshot>(
            getName(),
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            std::unordered_map<std::string, std::string>(),
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>());
    }
};

class ErrorStatusRocksDBVectorBatchStateAccessor : public RocksDBVectorBatchStateAccessor {
public:
    using RocksDBVectorBatchStateAccessor::RocksDBVectorBatchStateAccessor;

protected:
    rocksdb::Status readSerializedBatch(
        const rocksdb::ReadOptions& /*readOptions*/, const std::vector<int8_t>& /*key*/) override
    {
        return rocksdb::Status::IOError("injected non-ok status");
    }
};

void expectByteViewEquals(const ByteView& actual, const std::vector<int8_t>& expected)
{
    ASSERT_NE(actual.data(), nullptr);
    ASSERT_EQ(actual.size(), expected.size());
    EXPECT_EQ(std::memcmp(actual.data(), expected.data(), expected.size()), 0);
}

class RocksDBVectorBatchStateAccessorTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        fs::remove_all(fs::path(kDbRootPath));
        fs::create_directories(fs::path(kDbRootPath));
        dbPath_ = makeRocksDbPath();

        rocksdb::Options options;
        options.create_if_missing = true;
        options.create_missing_column_families = true;

        std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilies;
        columnFamilies.emplace_back("default", rocksdb::ColumnFamilyOptions());
        columnFamilies.emplace_back(kVectorBatchColumnFamily, rocksdb::ColumnFamilyOptions());

        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        rocksdb::Status status = rocksdb::DB::Open(options, dbPath_, columnFamilies, &handles, &db_);
        ASSERT_TRUE(status.ok()) << "Failed to open RocksDB: " << status.ToString();

        defaultCfHandle_ = handles[0];
        cfHandle_ = handles[1];
        resourceGuard_ = std::make_shared<ResourceGuard>();
    }

    void TearDown() override
    {
        releaseSnapshot();
        if (db_ != nullptr) {
            if (cfHandle_ != nullptr) {
                db_->DestroyColumnFamilyHandle(cfHandle_);
                cfHandle_ = nullptr;
            }
            if (defaultCfHandle_ != nullptr) {
                db_->DestroyColumnFamilyHandle(defaultCfHandle_);
                defaultCfHandle_ = nullptr;
            }
            db_->Close();
            delete db_;
            db_ = nullptr;
        }
        resourceGuard_.reset();
        fs::remove_all(fs::path(dbPath_));
    }

    void takeSnapshot()
    {
        releaseSnapshot();
        snapshot_ = db_->GetSnapshot();
        ASSERT_NE(snapshot_, nullptr);
    }

    void releaseSnapshot()
    {
        if (db_ != nullptr && snapshot_ != nullptr) {
            db_->ReleaseSnapshot(snapshot_);
            snapshot_ = nullptr;
        }
    }

    void writeVectorBatch(int64_t batchId, const std::vector<int8_t>& value)
    {
        std::vector<int8_t> key = makeVbKey(batchId, keyGroupPrefixBytes_, keyGroup_);
        rocksdb::Status status = db_->Put(
            rocksdb::WriteOptions(),
            cfHandle_,
            rocksdb::Slice(reinterpret_cast<const char*>(key.data()), key.size()),
            rocksdb::Slice(reinterpret_cast<const char*>(value.data()), value.size()));
        ASSERT_TRUE(status.ok()) << "Failed to write vector batch: " << status.ToString();
    }

    std::unique_ptr<RocksDBVectorBatchStateAccessor> createAccessor(
        VectorBatchAccessorOptions options = optionsWithCacheBytes(64 * 1024))
    {
        return std::make_unique<RocksDBVectorBatchStateAccessor>(
            db_, cfHandle_, snapshot_, keyGroupPrefixBytes_, keyGroup_, options);
    }

    std::shared_ptr<RocksDbKvStateInfo> createKvStateInfo(const std::string& stateName, rocksdb::ColumnFamilyHandle* cf)
    {
        return std::make_shared<RocksDbKvStateInfo>(cf, std::make_shared<TestRegisteredStateMetaInfo>(stateName));
    }

    std::shared_ptr<RocksDBFullSnapshotResources> createSnapshotResources(
        const std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>& kvStateInformation)
    {
        std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>>
            registeredPQStates;
        return RocksDBFullSnapshotResources::create(
            kvStateInformation,
            registeredPQStates,
            db_,
            resourceGuard_,
            &keyGroupRange_,
            nullptr,
            keyGroupPrefixBytes_);
    }

    std::shared_ptr<RocksDBFullSnapshotResources> createSnapshotResourcesFromMetadata(
        const std::vector<std::shared_ptr<RocksDbKvStateInfo>>& metaDataCopy,
        rocksdb::DB* db,
        const rocksdb::Snapshot* snapshot,
        KeyGroupRange* keyGroupRange)
    {
        return std::make_shared<RocksDBFullSnapshotResources>(
            nullptr,
            snapshot,
            metaDataCopy,
            std::vector<std::shared_ptr<StateMetaInfoSnapshot>>(),
            std::vector<std::unique_ptr<SingleStateIterator>>(),
            std::unordered_set<int>(),
            db,
            keyGroupPrefixBytes_,
            keyGroupRange,
            nullptr);
    }

    std::shared_ptr<RocksDBFullSnapshotResources> createSnapshotResourcesTakingFixtureSnapshot(
        const std::vector<std::shared_ptr<RocksDbKvStateInfo>>& metaDataCopy)
    {
        takeSnapshot();
        const rocksdb::Snapshot* snapshot = snapshot_;
        snapshot_ = nullptr;
        return createSnapshotResourcesFromMetadata(metaDataCopy, db_, snapshot, &keyGroupRange_);
    }

    std::shared_ptr<RocksDBFullSnapshotResources> createSnapshotResourcesTakingFixtureSnapshot(
        const std::vector<std::shared_ptr<RocksDbKvStateInfo>>& metaDataCopy,
        rocksdb::DB* db,
        KeyGroupRange* keyGroupRange)
    {
        takeSnapshot();
        const rocksdb::Snapshot* snapshot = snapshot_;
        snapshot_ = nullptr;
        return createSnapshotResourcesFromMetadata(metaDataCopy, db, snapshot, keyGroupRange);
    }

    rocksdb::DB* db_ = nullptr;
    rocksdb::ColumnFamilyHandle* defaultCfHandle_ = nullptr;
    rocksdb::ColumnFamilyHandle* cfHandle_ = nullptr;
    const rocksdb::Snapshot* snapshot_ = nullptr;
    std::shared_ptr<ResourceGuard> resourceGuard_;
    std::string dbPath_;
    int32_t keyGroupPrefixBytes_ =
        CompositeKeySerializationUtils::computeRequiredBytesInKeyGroupPrefix(kNumberOfKeyGroups);
    int32_t keyGroup_ = kKeyGroup;
    KeyGroupRange keyGroupRange_{keyGroup_, keyGroup_};
};

// getSerializedBatch 必须按配置/计算得到的 key-group prefix 宽度 + LongSerializer(batchId) 查找已有 VB。
// 本用例选择会产生 2-byte prefix 的 key group 总数，防止实现硬编码 1 byte。
TEST_F(RocksDBVectorBatchStateAccessorTest, GetSerializedBatchFindsExistingBatch)
{
    ASSERT_EQ(keyGroupPrefixBytes_, CompositeKeySerializationUtils::PREFIX_TWO_BYTES);
    std::vector<int8_t> expectedValue = serializeVectorBatch(1);
    writeVectorBatch(kBatchId, expectedValue);
    takeSnapshot();
    auto accessor = createAccessor();

    ByteView actualValue;
    ASSERT_TRUE(accessor->getSerializedBatch(kBatchId, &actualValue));

    expectByteViewEquals(actualValue, expectedValue);
}

// 缺失 batchId 应返回 false，不能把 RocksDB NotFound 升级成异常或返回空的成功 ByteView。
TEST_F(RocksDBVectorBatchStateAccessorTest, GetSerializedBatchReturnsFalseForMissingBatch)
{
    writeVectorBatch(kBatchId, serializeVectorBatch(1));
    takeSnapshot();
    auto accessor = createAccessor();

    ByteView value;

    EXPECT_FALSE(accessor->getSerializedBatch(404, &value));
}

// 构造函数必须拒绝空 DB、空列族或空 snapshot，避免后续读取路径在空依赖上崩溃。
TEST_F(RocksDBVectorBatchStateAccessorTest, ConstructorThrowsForNullDbColumnFamilyOrSnapshot)
{
    takeSnapshot();
    VectorBatchAccessorOptions options = optionsWithCacheBytes(64 * 1024);

    EXPECT_THROW(
        { RocksDBVectorBatchStateAccessor accessor(nullptr, cfHandle_, snapshot_, 1, 0, options); },
        std::runtime_error);
    EXPECT_THROW(
        { RocksDBVectorBatchStateAccessor accessor(db_, nullptr, snapshot_, 1, 0, options); }, std::runtime_error);
    EXPECT_THROW(
        { RocksDBVectorBatchStateAccessor accessor(db_, cfHandle_, nullptr, 1, 0, options); }, std::runtime_error);
}

// RocksDB 返回非 OK 且非 NotFound 状态时应抛出异常，测试通过子类注入错误状态避免依赖特定 RocksDB 版本行为。
TEST_F(RocksDBVectorBatchStateAccessorTest, GetSerializedBatchThrowsOnNonOkRocksStatus)
{
    takeSnapshot();
    ErrorStatusRocksDBVectorBatchStateAccessor accessor(
        db_, cfHandle_, snapshot_, keyGroupPrefixBytes_, keyGroup_, optionsWithCacheBytes(64 * 1024));

    ByteView value;
    EXPECT_THROW(accessor.getSerializedBatch(kBatchId, &value), std::runtime_error);
}

// 有效 batchId/rowId 应从 RocksDB 中的序列化 VB 反序列化出调用方独占持有的 RowData。
TEST_F(RocksDBVectorBatchStateAccessorTest, GetRowReturnsOwnedRowData)
{
    writeVectorBatch(kBatchId, serializeVectorBatch(2));
    takeSnapshot();
    auto accessor = createAccessor();

    std::unique_ptr<RowData> first = accessor->getRow(kBatchId, 1);
    std::unique_ptr<RowData> second = accessor->getRow(kBatchId, 1);

    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_NE(first.get(), second.get());
    EXPECT_EQ(first->getArity(), 0);
    EXPECT_EQ(second->getArity(), 0);
}

// accessor 绑定 RocksDB snapshot 后，后续同 key 更新不应影响已绑定 accessor 看到的 serialized VB。
TEST_F(RocksDBVectorBatchStateAccessorTest, GetRowUsesSnapshotValueWhenDbIsUpdatedAfterSnapshot)
{
    std::vector<int8_t> oldValue = serializeVectorBatch(1);
    std::vector<int8_t> newValue = serializeVectorBatch(2);
    ASSERT_NE(oldValue, newValue);

    writeVectorBatch(kBatchId, oldValue);
    takeSnapshot();
    auto accessor = createAccessor();
    writeVectorBatch(kBatchId, newValue);

    ByteView actualValue;
    ASSERT_TRUE(accessor->getSerializedBatch(kBatchId, &actualValue));
    expectByteViewEquals(actualValue, oldValue);
}

// decoded cache 容量小于 serialized VB 时仍应返回当前行，不能因为不可缓存而让读取失败。
TEST_F(RocksDBVectorBatchStateAccessorTest, OversizedBatchIsNotCachedButStillReturnsCurrentRow)
{
    std::vector<int8_t> serializedValue = serializeVectorBatch(1);
    writeVectorBatch(kBatchId, serializedValue);
    takeSnapshot();
    auto accessor = createAccessor(optionsWithCacheBytes(0));

    std::unique_ptr<RowData> first = accessor->getRow(kBatchId, 0);
    std::unique_ptr<RowData> second = accessor->getRow(kBatchId, 0);

    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_NE(first.get(), second.get());
    EXPECT_EQ(first->getArity(), 0);
    EXPECT_EQ(second->getArity(), 0);
}

// close 应清理 pinned slice 和 decoded cache，后续 serialized 查询失败且 getRow 返回 nullptr；snapshot 仍由调用方释放。
TEST_F(RocksDBVectorBatchStateAccessorTest, CloseClearsPinnedSliceAndCache)
{
    std::vector<int8_t> serializedValue = serializeVectorBatch(1);
    writeVectorBatch(kBatchId, serializedValue);
    takeSnapshot();
    auto accessor = createAccessor();

    ByteView value;
    ASSERT_TRUE(accessor->getSerializedBatch(kBatchId, &value));
    ASSERT_NE(accessor->getRow(kBatchId, 0), nullptr);

    accessor->close();

    EXPECT_FALSE(accessor->getSerializedBatch(kBatchId, &value));
    EXPECT_EQ(value.data(), nullptr);
    EXPECT_EQ(value.size(), 0);
    EXPECT_EQ(accessor->getRow(kBatchId, 0), nullptr);
}

// resources factory 必须把 logical state name 映射到同名加 vb 的 RocksDB column family，并让 accessor 读到 VB
// 侧表快照数据。
TEST_F(RocksDBVectorBatchStateAccessorTest, CreateVectorBatchStateAccessorMapsLogicalStateToVbColumnFamily)
{
    const std::string logicalStateName = "join_state";
    const std::string vbStateName = logicalStateName + "vb";
    std::vector<int8_t> expectedValue = serializeVectorBatch(1);
    writeVectorBatch(kBatchId, expectedValue);
    auto resources = createSnapshotResources(
        {{logicalStateName, createKvStateInfo(logicalStateName, defaultCfHandle_)},
         {vbStateName, createKvStateInfo(vbStateName, cfHandle_)}});

    auto accessor = resources->createVectorBatchStateAccessor(logicalStateName, optionsWithCacheBytes(64 * 1024));

    ASSERT_NE(accessor, nullptr);
    ByteView actualValue;
    ASSERT_TRUE(accessor->getSerializedBatch(kBatchId, &actualValue));
    expectByteViewEquals(actualValue, expectedValue);
}

// 调用方误传已经带 vb 后缀的名称时，resources factory 仍按 logical state 追加 vb，并因找不到 vbvb CF 返回 nullptr。
TEST_F(RocksDBVectorBatchStateAccessorTest, CreateVectorBatchStateAccessorReturnsNullForAlreadyVbStateName)
{
    const std::string vbStateName = "join_statevb";
    std::vector<int8_t> expectedValue = serializeVectorBatch(1);
    writeVectorBatch(kBatchId, expectedValue);
    auto resources = createSnapshotResources({{vbStateName, createKvStateInfo(vbStateName, cfHandle_)}});

    auto accessor = resources->createVectorBatchStateAccessor(vbStateName, optionsWithCacheBytes(64 * 1024));

    EXPECT_EQ(accessor, nullptr);
}

// 未注册对应 VB side table 时，resources factory 应返回 nullptr，由上层 save-state context 负责 fail-fast。
TEST_F(RocksDBVectorBatchStateAccessorTest, CreateVectorBatchStateAccessorReturnsNullForMissingVbState)
{
    const std::string logicalStateName = "join_state";
    auto resources =
        createSnapshotResources({{logicalStateName, createKvStateInfo(logicalStateName, defaultCfHandle_)}});

    EXPECT_EQ(resources->createVectorBatchStateAccessor(logicalStateName, optionsWithCacheBytes(64 * 1024)), nullptr);
    EXPECT_EQ(resources->createVectorBatchStateAccessor("unknown_state", optionsWithCacheBytes(64 * 1024)), nullptr);
}

// resources factory 遇到空 metadata、空 metadata entry 或 metadata 内空 column family 时必须返回 nullptr。
TEST_F(RocksDBVectorBatchStateAccessorTest, CreateVectorBatchStateAccessorReturnsNullForInvalidMetadata)
{
    const std::string logicalStateName = "join_state";
    const std::string vbStateName = logicalStateName + "vb";
    auto emptyMetadataResources =
        createSnapshotResourcesTakingFixtureSnapshot(std::vector<std::shared_ptr<RocksDbKvStateInfo>>());
    auto nullMetadataResources =
        createSnapshotResourcesTakingFixtureSnapshot(std::vector<std::shared_ptr<RocksDbKvStateInfo>>{nullptr});
    auto nullColumnFamilyResources = createSnapshotResourcesTakingFixtureSnapshot(
        std::vector<std::shared_ptr<RocksDbKvStateInfo>>{createKvStateInfo(vbStateName, nullptr)});

    EXPECT_EQ(
        emptyMetadataResources->createVectorBatchStateAccessor(logicalStateName, optionsWithCacheBytes(64 * 1024)),
        nullptr);
    EXPECT_EQ(
        nullMetadataResources->createVectorBatchStateAccessor(logicalStateName, optionsWithCacheBytes(64 * 1024)),
        nullptr);
    EXPECT_EQ(
        nullColumnFamilyResources->createVectorBatchStateAccessor(logicalStateName, optionsWithCacheBytes(64 * 1024)),
        nullptr);
}

// resources factory 遇到空 DB、空 snapshot、空 keyGroupRange 或空 key-group range 时必须返回 nullptr，
// 不能在创建 accessor 前崩溃。
TEST_F(RocksDBVectorBatchStateAccessorTest, CreateVectorBatchStateAccessorReturnsNullForInvalidRuntimeResources)
{
    const std::string logicalStateName = "join_state";
    const std::string vbStateName = logicalStateName + "vb";
    std::vector<std::shared_ptr<RocksDbKvStateInfo>> metadata{createKvStateInfo(vbStateName, cfHandle_)};

    takeSnapshot();
    const rocksdb::Snapshot* snapshot = snapshot_;
    auto nullDbResources = createSnapshotResourcesFromMetadata(metadata, nullptr, snapshot, &keyGroupRange_);
    EXPECT_EQ(
        nullDbResources->createVectorBatchStateAccessor(logicalStateName, optionsWithCacheBytes(64 * 1024)), nullptr);
    nullDbResources.reset();
    releaseSnapshot();

    auto nullSnapshotResources = createSnapshotResourcesFromMetadata(metadata, db_, nullptr, &keyGroupRange_);
    auto nullKeyGroupRangeResources = createSnapshotResourcesTakingFixtureSnapshot(metadata, db_, nullptr);
    KeyGroupRange emptyKeyGroupRange;
    auto emptyKeyGroupRangeResources = createSnapshotResourcesTakingFixtureSnapshot(metadata, db_, &emptyKeyGroupRange);

    EXPECT_EQ(
        nullSnapshotResources->createVectorBatchStateAccessor(logicalStateName, optionsWithCacheBytes(64 * 1024)),
        nullptr);
    EXPECT_EQ(
        nullKeyGroupRangeResources->createVectorBatchStateAccessor(logicalStateName, optionsWithCacheBytes(64 * 1024)),
        nullptr);
    EXPECT_EQ(
        emptyKeyGroupRangeResources->createVectorBatchStateAccessor(logicalStateName, optionsWithCacheBytes(64 * 1024)),
        nullptr);
}

// resources 创建的 accessor 不拥有 RocksDB snapshot：关闭 accessor 后 resources 仍负责释放 snapshot，DB/CF
// 生命周期不应被 accessor close 破坏。
TEST_F(RocksDBVectorBatchStateAccessorTest, AccessorCreatedByResourcesReadsFromResourcesSnapshot)
{
    const std::string logicalStateName = "join_state";
    const std::string vbStateName = logicalStateName + "vb";
    std::vector<int8_t> snapshotValue = serializeVectorBatch(1);
    std::vector<int8_t> newerValue = serializeVectorBatch(2);
    ASSERT_NE(snapshotValue, newerValue);
    writeVectorBatch(kBatchId, snapshotValue);
    auto resources = createSnapshotResources(
        {{logicalStateName, createKvStateInfo(logicalStateName, defaultCfHandle_)},
         {vbStateName, createKvStateInfo(vbStateName, cfHandle_)}});

    auto accessor = resources->createVectorBatchStateAccessor(logicalStateName, optionsWithCacheBytes(64 * 1024));
    ASSERT_NE(accessor, nullptr);
    writeVectorBatch(kBatchId, newerValue);

    ByteView actualValue;
    ASSERT_TRUE(accessor->getSerializedBatch(kBatchId, &actualValue));
    expectByteViewEquals(actualValue, snapshotValue);

    accessor->close();
    ASSERT_NO_FATAL_FAILURE(writeVectorBatch(kBatchId + 1, newerValue));

    resources->cleanup();
    ASSERT_NO_FATAL_FAILURE(writeVectorBatch(kBatchId + 2, newerValue));
}

} // namespace
