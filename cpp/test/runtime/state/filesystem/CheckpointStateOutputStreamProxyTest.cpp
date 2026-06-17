/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <jni.h>
#include <memory>
#include <stdexcept>
#include <vector>

#include "core/utils/ByteView.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/CheckpointType.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/state/CheckpointStateOutputStreamProxy.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/StreamStateHandle.h"
#include "test/runtime/state/MockSavepointBridge.h"

using ::testing::_;
using ::testing::Return;
using ::testing::NiceMock;

namespace {

/**
 * CheckpointStateOutputStreamProxy 测试夹具。
 * 使用共享 MockSavepointBridge + CheckpointOptions 初始化。
 * NiceMock 自动静默未预期的析构调用（ReleaseSavepointOutputDirectBuffer 等），
 * 各测试仅对需要验证的方法设置 EXPECT_CALL。
 */
class CheckpointStateOutputStreamProxyTest : public ::testing::Test {
protected:
    void SetUp() override {
        mockBridge_ = std::make_shared<NiceMock<MockSavepointBridge>>();
        auto* savepointType = SavepointType::savepoint(SavepointFormatType::CANONICAL);
        checkpointOptions_ = CheckpointOptions::AlignedNoTimeout(
            *savepointType, CheckpointStorageLocationReference::GetDefault());
    }

    /** 配置 mock 以支持 Proxy 正常构造：Acquire 返回有效 provider。 */
    void ExpectProxyConstruction() {
        EXPECT_CALL(*mockBridge_, AcquireSavepointOutputStream(_, _))
            .WillOnce(Return(kMockProvider));
    }

    /** 配置 mock 以支持 Proxy 在无 DirectByteBuffer 情况下构造（走 fallback 路径）。 */
    void ExpectProxyConstructionNoDirectBuffer() {
        EXPECT_CALL(*mockBridge_, AcquireSavepointOutputStream(_, _))
            .WillOnce(Return(kMockProvider));
        EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
            .WillOnce(Return(static_cast<jobject>(nullptr)));
    }

    std::shared_ptr<NiceMock<MockSavepointBridge>> mockBridge_;
    CheckpointOptions *checkpointOptions_ = nullptr;
};

// ---- 构造 / 析构 ----

/**
 * 正常构造：AcquireSavepointOutputStream 和 CreateSavepointOutputDirectBuffer 成功，
 * 析构时自动调用 ReleaseSavepointOutputDirectBuffer（由 NiceMock 静默处理）。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, ConstructWithValidBridge) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));

    {
        CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    }
}

/**
 * AcquireSavepointOutputStream 返回 null 时，构造应抛出 std::runtime_error。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, ConstructThrowsWhenAcquireFails) {
    EXPECT_CALL(*mockBridge_, AcquireSavepointOutputStream(_, _))
        .WillOnce(Return(static_cast<jobject>(nullptr)));

    EXPECT_THROW(
        { CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_); },
        std::runtime_error);
}

// ---- 位置追踪 ----

/**
 * writeByte/writeShort/writeInt/writeLong 写入后 pos 应正确递增。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, WriteAdvancesPos) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    proxy.writeByte(0x42);
    EXPECT_EQ(proxy.getPos(), 1);
    proxy.writeShort(0x0102);
    EXPECT_EQ(proxy.getPos(), 3);
    proxy.writeInt(0x01020304);
    EXPECT_EQ(proxy.getPos(), 7);
    proxy.writeLong(0x0102030405060708LL);
    EXPECT_EQ(proxy.getPos(), 15);
}

/**
 * writeBytes 写入指定长度的数据后 pos 应与写入长度一致。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, WriteBytesAdvancesPos) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    const int8_t data[] = {1, 2, 3, 4, 5};
    proxy.writeBytes(data, 5);
    EXPECT_EQ(proxy.getPos(), 5);
}

// ---- flush 行为 ----

/**
 * 有 DirectByteBuffer 时 flush 走 WriteSavepointOutputStreamDirect 路径。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, FlushTriggersDirectWriteWhenDirectBufferExists) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, WriteSavepointOutputStreamDirect(kMockProvider, kMockDirectBuffer, 4))
        .WillOnce(Return(true));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    proxy.writeInt(0x12345678);
    proxy.flush();
    EXPECT_EQ(proxy.getPos(), 4);
}

/**
 * 无 DirectByteBuffer 时 flush 回退到 WriteSavepointOutputStream 路径。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, FlushUsesFallbackWhenNoDirectBuffer) {
    ExpectProxyConstructionNoDirectBuffer();
    EXPECT_CALL(*mockBridge_, WriteSavepointOutputStream(kMockProvider, _, 0, 4)).Times(1);

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    proxy.writeInt(0x12345678);
    proxy.flush();
    EXPECT_EQ(proxy.getPos(), 4);
}

// ---- BytePatch 守卫状态机 ----

/**
 * 正常 BytePatch 流程：tryWrite → patchByte → releasePatch。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, BytePatchPatchAndRelease) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    const int8_t key[] = {1};
    const int8_t value[] = {2, 3};
    CheckpointStateOutputStreamProxy::BytePatch patch;
    bool ok = proxy.tryWritePatchableKeyValuePair(
        ByteView::fromBuffer(key, 1),
        ByteView::fromBuffer(value, 2),
        patch);
    EXPECT_TRUE(ok);
    EXPECT_TRUE(patch.valid);

    proxy.patchByte(patch, 0x80);
    proxy.releasePatch(patch);
}

/**
 * 上一个 BytePatch 未 release 前再次 tryWrite 应抛出异常。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, BytePatchDoubleActivationThrows) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    const int8_t key1[] = {1};
    const int8_t value1[] = {2};
    CheckpointStateOutputStreamProxy::BytePatch patch1;
    proxy.tryWritePatchableKeyValuePair(
        ByteView::fromBuffer(key1, 1),
        ByteView::fromBuffer(value1, 1),
        patch1);

    const int8_t key2[] = {3};
    const int8_t value2[] = {4};
    CheckpointStateOutputStreamProxy::BytePatch patch2;
    EXPECT_THROW(
        proxy.tryWritePatchableKeyValuePair(
            ByteView::fromBuffer(key2, 1),
            ByteView::fromBuffer(value2, 1),
            patch2),
        std::runtime_error);
}

/**
 * 未激活 BytePatch 守卫时调用 patchByte 应抛出异常。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, PatchByteWithoutActiveGuardThrows) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    CheckpointStateOutputStreamProxy::BytePatch invalidPatch;
    EXPECT_THROW(proxy.patchByte(invalidPatch, 0x80), std::runtime_error);
}

/**
 * BytePatch 激活期间 flush 被阻塞抛异常；release 后 flush 成功，再 patchByte 抛异常。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, PatchByteAfterFlushThrows) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, WriteSavepointOutputStreamDirect(_, _, _))
        .WillRepeatedly(Return(true));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    const int8_t key[] = {1};
    const int8_t value[] = {2};
    CheckpointStateOutputStreamProxy::BytePatch patch;
    proxy.tryWritePatchableKeyValuePair(
        ByteView::fromBuffer(key, 1),
        ByteView::fromBuffer(value, 1),
        patch);

    // 激活期间 flush 被阻断
    EXPECT_THROW(proxy.flush(), std::runtime_error);

    // release 后 flush 成功，patch 失效
    proxy.releasePatch(patch);
    proxy.flush();

    // 已失效的 patch 不能再 patchByte
    EXPECT_THROW(proxy.patchByte(patch, 0x80), std::runtime_error);
}

/**
 * BytePatch 异常路径：未激活时 patchByte 抛异常；
 * 重复 releasePatch 抛异常；激活期间 writeBytes 被阻塞抛异常。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, BytePatchGuardErrorPaths) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    // 未激活守卫时 patchByte 抛异常
    CheckpointStateOutputStreamProxy::BytePatch invalidPatch;
    EXPECT_THROW(proxy.patchByte(invalidPatch, 0x80), std::runtime_error);

    // 正常激活后重复 releasePatch 抛异常
    const int8_t key[] = {1};
    const int8_t value[] = {2};
    CheckpointStateOutputStreamProxy::BytePatch patch;
    proxy.tryWritePatchableKeyValuePair(
        ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), patch);
    proxy.releasePatch(patch);
    EXPECT_THROW(proxy.releasePatch(patch), std::runtime_error);

    // 激活期间 writeBytes 被阻塞
    proxy.tryWritePatchableKeyValuePair(
        ByteView::fromBuffer(key, 1), ByteView::fromBuffer(value, 1), patch);
    const int8_t data[] = {0x42};
    EXPECT_THROW(proxy.writeBytes(data, 1), std::runtime_error);
}

// ---- KeyValuePair 写入 ----

/**
 * writeKeyValuePair(vector 重载) 写入 key/value 并正确更新 pos。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, WriteKeyValuePairUsesVectorOverload) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    std::vector<int8_t> key = {1, 2};
    std::vector<int8_t> value = {3, 4, 5};
    proxy.writeKeyValuePair(key, value);
    EXPECT_EQ(proxy.getPos(), static_cast<size_t>(4 + 2 + 4 + 3));
}

/**
 * tryWritePatchableKeyValuePair 的 key 为空时应抛异常。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, TryWritePatchableFailsWhenKeyEmpty) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    const int8_t value[] = {1};
    CheckpointStateOutputStreamProxy::BytePatch patch;
    EXPECT_THROW(
        proxy.tryWritePatchableKeyValuePair(ByteView(), ByteView::fromBuffer(value, 1), patch),
        std::runtime_error);
}

// ---- close ----

/**
 * close 会先 flush 再调用 CloseSavepointOutputStream 并返回非空 handle。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, CloseFlushesAndReturnsHandle) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, WriteSavepointOutputStreamDirect(_, _, _)).WillRepeatedly(Return(true));
    EXPECT_CALL(*mockBridge_, CloseSavepointOutputStream(kMockProvider))
        .WillOnce(Return(SnapshotResult<StreamStateHandle>::Empty()));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    proxy.writeInt(0xDEADBEEF);

    auto handle = proxy.close();
    EXPECT_NE(handle, nullptr);
}

// ---- writeMetadata / writeOperatorMetaData ----

/**
 * writeMetadata 调用后 pos 应与 bridge 返回的 stream 位置一致。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, WriteMetadataUpdatesPos) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, WriteSavepointMetadata(_, _, _)).Times(1);
    EXPECT_CALL(*mockBridge_, GetSavepointOutputStreamPos(kMockProvider))
        .WillOnce(Return(1024L));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> snapshots;
    proxy.writeMetadata(snapshots, "testSerializer");
    EXPECT_EQ(proxy.getPos(), 1024);
}

/**
 * prepareForPatchableKeyValuePair 在大容量需求时应触发 growBuffer。
 */
TEST_F(CheckpointStateOutputStreamProxyTest, PrepareForPatchableGrowsBuffer) {
    ExpectProxyConstruction();
    EXPECT_CALL(*mockBridge_, CreateSavepointOutputDirectBuffer(_, _))
        .WillOnce(Return(kMockDirectBuffer))
        .WillOnce(Return(kMockDirectBuffer));
    EXPECT_CALL(*mockBridge_, WriteSavepointOutputStreamDirect(_, _, _))
        .WillRepeatedly(Return(true));

    CheckpointStateOutputStreamProxy proxy(mockBridge_, 1L, checkpointOptions_);

    for (int i = 0; i < 100; i++) {
        proxy.writeInt(0x12345678);
    }
    size_t posBeforePrepare = proxy.getPos();

    proxy.prepareForPatchableKeyValuePair(128 * 1024);

    EXPECT_EQ(proxy.getPos(), posBeforePrepare);
}

}  // namespace
