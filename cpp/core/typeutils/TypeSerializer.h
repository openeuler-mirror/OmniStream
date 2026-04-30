/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * @Description: Type Serializer
 */
#ifndef FLINK_TNEL_TYPESERIALIZER_H
#define FLINK_TNEL_TYPESERIALIZER_H

#include <memory/DataOutputSerializer.h>

#include "../memory/DataInputView.h"
#include "OmniOperatorJIT/core/src/type/data_type.h"

#include "streaming/runtime/streamrecord/StreamElement.h"
#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "basictypes/Object.h"
#include "TypeSerializerSnapshot.h"

enum class BackendDataType {
    BIGINT_BK,
    INT_BK,
    XXHASH128_BK, // For Join
    TUPLE_INT32_INT64, // For Join
    TUPLE_INT32_INT32_INT64, // For OuterJoin
    VOID_NAMESPACE_BK,
    VARCHAR_BK,
    TIME_WINDOW_BK,
    ROW_BK,
    TUPLE_OBJ_OBJ_BK, // for Datastream Tuple
    OBJECT_BK,
    LONG_BK,
    ROW_LIST_BK,
    INVALID_BK,
    POJO_BK,
    SET_LONG
};

inline std::ostream& operator<<(std::ostream& os, const BackendDataType& type) {
    switch (type) {
        case BackendDataType::BIGINT_BK: return os << "BIGINT_BK";
        case BackendDataType::INT_BK: return os << "INT_BK";
        case BackendDataType::XXHASH128_BK: return os << "XXHASH128_BK";
        case BackendDataType::TUPLE_INT32_INT64: return os << "TUPLE_INT32_INT64";
        case BackendDataType::TUPLE_INT32_INT32_INT64: return os << "TUPLE_INT32_INT32_INT64";
        case BackendDataType::VOID_NAMESPACE_BK: return os << "VOID_NAMESPACE_BK";
        case BackendDataType::VARCHAR_BK: return os << "VARCHAR_BK";
        case BackendDataType::TIME_WINDOW_BK: return os << "TIME_WINDOW_BK";
        case BackendDataType::ROW_BK: return os << "ROW_BK";
        case BackendDataType::TUPLE_OBJ_OBJ_BK: return os << "TUPLE_OBJ_OBJ_BK";
        case BackendDataType::OBJECT_BK: return os << "OBJECT_BK";
        case BackendDataType::LONG_BK: return os << "LONG_BK";
        case BackendDataType::ROW_LIST_BK: return os << "ROW_LIST_BK";
        case BackendDataType::INVALID_BK: return os << "INVALID_BK";
        case BackendDataType::POJO_BK: return os << "POJO_BK";
        default: return os << "UNKNOWN_BK";
    }
}

class TypeSerializer {
public:

    // TypeSerializer(int id) : dataId(id){}

    /**
     * De-serializes a record from the given source input view.
     *
     * @param source The input view from which to read the data.
     * @return The deserialized element_.
     * @throws IOException Thrown, if the de-serialization encountered an I/O related error.
     *     Typically raised by the input view, which may have an underlying I/O channel from which
     *     it reads.
     */
    virtual void* deserialize(DataInputView& source)
    {
        return nullptr;
    }
    virtual void serialize(void* record, DataOutputSerializer& target)
    {
    }

    // new interface for DataStream
    virtual void deserialize(Object *buffer, DataInputView& source) {};
    virtual void serialize(Object *buffer, DataOutputSerializer& target) {};
    virtual Object* GetBuffer()
    {
        return nullptr;
    }

    virtual const char* getName() const;
    virtual BackendDataType getBackendId() const
    {
        return BackendDataType::INVALID_BK;
    }

    virtual ~TypeSerializer()
    {
        if (reuseBuffer != nullptr) {
            reuseBuffer->putRefCount();
            reuseBuffer = nullptr;
        }
    }

    void setSelfBufferReusable(bool bufferReusable_)
    {
        bufferReusable = bufferReusable_;
        setSubBufferReusable(bufferReusable_);
    }

    virtual void setSubBufferReusable(bool bufferReusable_) {}

    bool isReusable()
    {
        return bufferReusable;
    }

    virtual TypeSerializer* duplicate()
    {
        // Only VoidNamespaceSerializer, MapSerializer, KyroSerializer are used in MT6000C
        NOT_IMPL_EXCEPTION
    };
    virtual std::shared_ptr<TypeSerializerSnapshot> snapshotConfiguration()
    {
        // Only VoidNamespaceSerializer, MapSerializer, KyroSerializer are used in MT6000C
        NOT_IMPL_EXCEPTION
    };

    virtual std::string toJson()
    {
        return "";
    }

protected:
    Object* reuseBuffer = nullptr;
    bool bufferReusable = false;
};
#endif
