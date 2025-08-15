//
// Created by root on 9/11/24.
//

#ifndef FLINK_TNEL_INTERNALTYPEINFO_H
#define FLINK_TNEL_INTERNALTYPEINFO_H

#include "../types/logical/LogicalType.h"
#include "../../core/typeutils/TypeSerializer.h"
#include "../../core/typeinfo/TypeInformation.h"
#include "../types/logical/RowType.h"

class InternalTypeInfo : public TypeInformation {

public:
    InternalTypeInfo(LogicalType *type, TypeSerializer *typeSerializer);

    TypeSerializer *createTypeSerializer(std::string) override;

    std::string name() override;

    ~InternalTypeInfo() override;

    static InternalTypeInfo *of(LogicalType *type);

    static InternalTypeInfo *ofRowType(RowType *type);

    static InternalTypeInfo *INT_TYPE;

    //omniruntime::type::DataTypeId getDataId() const override { return typeSerializer == nullptr? type_ == nullptr ? omniruntime::type::OMNI_INVALID : static_cast<omniruntime::type::DataTypeId>(type_->getTypeId()) : typeSerializer->getDataId();};

    TypeSerializer* getTypeSerializer()
    {
        return typeSerializer;
    }

    BackendDataType getBackendId() const override
    {
        if(typeSerializer != nullptr) {
            return typeSerializer->getBackendId();
        } else {
            return BackendDataType::INVALID_BK;
        }
    }

private:

    LogicalType* type_;
    TypeSerializer* typeSerializer;
};


#endif //FLINK_TNEL_INTERNALTYPEINFO_H


/**
*   {
        final Class<?> typeClass = LogicalTypeUtils.toInternalConversionClass(type);
        final TypeSerializer<?> serializer = InternalSerializers.create(type);
        return (InternalTypeInfo<T>) new InternalTypeInfo(type, typeClass, serializer);
    }
*/