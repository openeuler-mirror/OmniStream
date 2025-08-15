/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Type Serializer
 */
#ifndef FLINK_TNEL_TYPESERIALIZER_H
#define FLINK_TNEL_TYPESERIALIZER_H

#include <io/DataOutputSerializer.h>

#include "../io/DataInputView.h"
#include "OmniOperatorJIT/core/src/type/data_type.h"

#include "functions/StreamElement.h"
#include "OmniOperatorJIT/core/src/type/data_type.h"
#include "basictypes/Object.h"

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
    INVALID_BK
};

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
    virtual void* deserialize(DataInputView& source) = 0;
    virtual void serialize(void* record, DataOutputSerializer& target) = 0;

    // new interface for DataStream
    virtual void deserialize(Object *buffer, DataInputView& source) {};
    virtual void serialize(Object *buffer, DataOutputSerializer& target) {};
    virtual Object* GetBuffer()
    {
        return nullptr;
    }

    virtual const char* getName() const;
    virtual BackendDataType getBackendId() const = 0;

    virtual ~TypeSerializer() {}
};


#endif  //FLINK_TNEL_TYPESERIALIZER_H
