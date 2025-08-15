#include <gtest/gtest.h>
#include "core/type/StringValue.h"
#include "io/DataOutputSerializer.h"
#include "io/DataInputDeserializer.h"
#include "core/utils/SysDataInput.h"
#include "io/NonSpanningWrapper.h"
#include <string>
#include <cstring>
#include <limits>

static const int HIGH_BIT = 0x1 << 7;
static const int HIGH_BIT14 = 0x1 << 14;
static const int HIGH_BIT21 = 0x1 << 21;
static const int HIGH_BIT28 = 0x1 << 28;

// 测试用例定义
TEST(StringValueTest, WriteAndReadTest) {
    StringValue stringValue;
    std::u32string value(U"Hello, World!");
    stringValue.setValue(value);

    // 写入数据到 DataOutputSerializer
    DataOutputSerializer out{};
    uint8_t *data = reinterpret_cast<uint8_t *>(malloc(100));
    out.setBackendBuffer(data, 100);
    stringValue.write(out);

    // 读取数据从 DataInputView
    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(out.getData(), 100);
    StringValue stringValue2;
    stringValue2.read(in);

    EXPECT_EQ(stringValue.getValue(), stringValue2.getValue());
    free(out.getData());
}

TEST(StringValueTest, WriteAndReadLargeStringTest) {
    std::string largeString(1000, 'A');
    StringValue stringValue;
    std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> convert;
    std::u32string u32str = convert.from_bytes(largeString);
    stringValue.setValue(u32str);

    // 写入数据到 DataOutputSerializer
    DataOutputSerializer out{};
    uint8_t *data = reinterpret_cast<uint8_t *>(malloc(100));
    OutputBufferStatus outputBufferStatus_;
    out.setBackendBuffer(&outputBufferStatus_);
    stringValue.write(out);

    // 读取数据从 DataInputView
    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(out.getData(), 100);
    StringValue stringValue2;
    stringValue2.read(in);

    EXPECT_EQ(stringValue.getValue(), stringValue2.getValue());
    free(out.getData());
}

TEST(StringValueTest, WriteStringTest) {
    String buffer("Hello, World!", 1024);
    DataOutputSerializer out{};
    uint8_t *data = reinterpret_cast<uint8_t *>(malloc(100));
    OutputBufferStatus outputBufferStatus_;
    out.setBackendBuffer(&outputBufferStatus_);

    StringValue::writeString(&buffer, out);
    data = out.getData();

    // 验证写入的数据是否正确
    size_t len = buffer.getValue().size();
    size_t expectedLen = len + 1; // Length is offset by one

    /*// 验证长度
    EXPECT_EQ(data[0], static_cast<uint8_t>(expectedLen));

    // 验证内容
    for (size_t i = 0; i < len; ++i) {
        EXPECT_EQ(data[i + 1], static_cast<uint8_t>(buffer.getValue()[i]));
    }*/
    free(out.getData());
}

TEST(StringValueTest, WriteStringLargeTest) {
    std::string largeString(1000, 'A');
    String buffer(largeString);
    DataOutputSerializer out{};
    uint8_t *data = reinterpret_cast<uint8_t *>(malloc(100));
    OutputBufferStatus outputBufferStatus_;
    out.setBackendBuffer(&outputBufferStatus_);

    StringValue::writeString(&buffer, out);
    data = out.getData();

    // 验证写入的数据是否正确
    size_t len = buffer.getValue().size();
    size_t expectedLen = len + 1; // Length is offset by one


    // 验证长度
    if (expectedLen < HIGH_BIT) {
        EXPECT_EQ(data[0], static_cast<uint8_t>(expectedLen));
    } else if (expectedLen < HIGH_BIT14) {
        EXPECT_EQ(data[0], static_cast<uint8_t>(expectedLen | HIGH_BIT));
        EXPECT_EQ(data[1], static_cast<uint8_t>(expectedLen >> 7));
    } else if (expectedLen < HIGH_BIT21) {
        EXPECT_EQ(data[0], static_cast<uint8_t>(expectedLen | HIGH_BIT));
        EXPECT_EQ(data[1], static_cast<uint8_t>((expectedLen >> 7) | HIGH_BIT));
        EXPECT_EQ(data[2], static_cast<uint8_t>(expectedLen >> 14));
    } else if (expectedLen < HIGH_BIT28) {
        EXPECT_EQ(data[0], static_cast<uint8_t>(expectedLen | HIGH_BIT));
        EXPECT_EQ(data[1], static_cast<uint8_t>((expectedLen >> 7) | HIGH_BIT));
        EXPECT_EQ(data[2], static_cast<uint8_t>((expectedLen >> 14) | HIGH_BIT));
        EXPECT_EQ(data[3], static_cast<uint8_t>(expectedLen >> 21));
    } else {
        EXPECT_EQ(data[0], static_cast<uint8_t>(expectedLen | HIGH_BIT));
        EXPECT_EQ(data[1], static_cast<uint8_t>((expectedLen >> 7) | HIGH_BIT));
        EXPECT_EQ(data[2], static_cast<uint8_t>((expectedLen >> 14) | HIGH_BIT));
        EXPECT_EQ(data[3], static_cast<uint8_t>((expectedLen >> 21) | HIGH_BIT));
        EXPECT_EQ(data[4], static_cast<uint8_t>(expectedLen >> 28));
    }
}

TEST(StringValueTest, ReadStringTest) {
    std::string expectedValue = "Hello, World!";
    // 计算长度并填充到前缀中
    size_t len = expectedValue.size();
    size_t prefixLength = len < HIGH_BIT ? 1 : (len < HIGH_BIT14 ? 2 : (len < HIGH_BIT21 ? 3 : (len < HIGH_BIT28 ? 4 : 5)));

    std::vector<uint8_t> data(prefixLength);
    size_t idx = 0;
    if (len < HIGH_BIT) {
        data[idx++] = static_cast<uint8_t>(len + 1);
    } else if (len < HIGH_BIT14) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 7);
    } else if (len < HIGH_BIT21) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 14);
    } else if (len < HIGH_BIT28) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 14) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 21);
    } else {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 14) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 21) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 28);
    }

    // 添加字符串内容
    data.insert(data.end(), expectedValue.begin(), expectedValue.end());

    String buffer("", data.size());
    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(data.data(), 100);

    StringValue::readString(&buffer, in);

    EXPECT_EQ(buffer.getValue(), expectedValue);
}

TEST(StringValueTest, ReadStringLargeTest) {
    std::string expectedValue(1000, 'A');
    // 计算长度并填充到前缀中
    size_t len = expectedValue.size();
    size_t prefixLength = len < HIGH_BIT ? 1 : (len < HIGH_BIT14 ? 2 : (len < HIGH_BIT21 ? 3 : (len < HIGH_BIT28 ? 4 : 5)));

    std::vector<uint8_t> data(prefixLength);
    size_t idx = 0;
    if (len < HIGH_BIT) {
        data[idx++] = static_cast<uint8_t>(len + 1);
    } else if (len < HIGH_BIT14) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 7);
    } else if (len < HIGH_BIT21) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 14);
    } else if (len < HIGH_BIT28) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 14) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 21);
    } else {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 14) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 21) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 28);
    }

    // 添加字符串内容
    data.insert(data.end(), expectedValue.begin(), expectedValue.end());

    String buffer("", data.size());
    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(data.data(), data.size());

    StringValue::readString(&buffer, in);

    EXPECT_EQ(buffer.getValue(), expectedValue);
}

TEST(StringValueTest, ReadStringNullTest) {
    // 创建一个表示空字符串的数据vector（长度为0）
    std::vector<uint8_t> data = {0};

    String buffer("", 1);
    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(data.data(), data.size());

    StringValue::readString(&buffer, in);

    EXPECT_EQ(buffer.getSize(), 1);
}

TEST(StringValueTest, ReadStringReallocateMemoryTest) {
    std::string expectedValue(1000, 'A');
    // 计算长度并填充到前缀中
    size_t len = expectedValue.size();
    size_t prefixLength = len < HIGH_BIT ? 1 : (len < HIGH_BIT14 ? 2 : (len < HIGH_BIT21 ? 3 : (len < HIGH_BIT28 ? 4 : 5)));

    std::vector<uint8_t> data(prefixLength);
    size_t idx = 0;
    if (len < HIGH_BIT) {
        data[idx++] = static_cast<uint8_t>(len + 1);
    } else if (len < HIGH_BIT14) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 7);
    } else if (len < HIGH_BIT21) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 14);
    } else if (len < HIGH_BIT28) {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 14) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 21);
    } else {
        data[idx++] = static_cast<uint8_t>((len + 1) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 7) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 14) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>(((len + 1) >> 21) | HIGH_BIT);
        data[idx++] = static_cast<uint8_t>((len + 1) >> 28);
    }

    // 添加字符串内容
    data.insert(data.end(), expectedValue.begin(), expectedValue.end());

    String buffer("", 1); // 初始容量很小，确保需要重新分配内存
    omnistream::datastream::NonSpanningWrapper in;
    in.initializeFromMemoryBuffer(data.data(), data.size());

    StringValue::readString(&buffer, in);

    EXPECT_EQ(buffer.getValue(), expectedValue);
    EXPECT_EQ(buffer.getSize(), len);
}