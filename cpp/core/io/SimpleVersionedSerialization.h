#ifndef OMNISTREAM_SIMPLEVERSIONEDSERIALIZATION_H
#define OMNISTREAM_SIMPLEVERSIONEDSERIALIZATION_H

#include <vector>
#include <stdexcept>
#include <memory>
#include "SimpleVersionedSerializer.h"

class SimpleVersionedSerialization {
public:
    // 写入版本号并序列化对象
    template <typename T>
    static std::vector<uint8_t> writeVersionAndSerialize(
            SimpleVersionedSerializer<T>& serializer, const T& obj) {
        // 简化实现：先写入版本号（4字节），然后写入序列化数据
        std::vector<uint8_t> result;
        int version = serializer.getVersion();

        // 写入版本号（大端序）
        result.push_back((version >> 24) & 0xFF);
        result.push_back((version >> 16) & 0xFF);
        result.push_back((version >> 8) & 0xFF);
        result.push_back(version & 0xFF);

        // 写入序列化数据
        std::vector<uint8_t> serialized = serializer.serialize(obj);
        int length = serialized.size();
        // 写入序列化数据长度（大端序）
        result.push_back((length >> 24) & 0xFF);
        result.push_back((length >> 16) & 0xFF);
        result.push_back((length >> 8) & 0xFF);
        result.push_back(length & 0xFF);

        result.insert(result.end(), serialized.begin(), serialized.end());
        std::stringstream ss;
        ss << "SimpleVersionedSerialization::writeVersionAndSerialize | version=" << version
            << " | length=" << length
            << " | serialized.size()=" << serialized.size()
            << std::endl;
        INFO_RELEASE(ss.str());

        std::stringstream dd;
        for (auto byte : result) {
            dd << (int)(byte) << ", ";
        }
        INFO_RELEASE("SimpleVersionedSerialization::writeVersionAndSerialize | bytes=[" << dd.str() << "]");

        return result;
    }
    
    // 读取版本号并反序列化对象
    template <typename T>
    static T* readVersionAndDeSerialize(
            SimpleVersionedSerializer<T>& serializer, std::vector<uint8_t>& bytes) {
        // 简化实现：先读取版本号（4字节），然后反序列化数据
        if (bytes.size() < 4) {
            throw std::runtime_error("Insufficient data for version deserialization");
        }

        // 读取版本号（大端序）
        int version = (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3];

        // 提取序列化数据部分
        std::vector<uint8_t> serializedData(bytes.begin() + 4, bytes.end());

        // 反序列化对象
        return serializer.deserialize(version, serializedData);
    }
};

#endif // OMNISTREAM_SIMPLEVERSIONEDSERIALIZATION_H
