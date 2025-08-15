/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Tuple Type Info for DataStream
 */
#ifndef OMNISTREAM_TUPLETYPEINFO_H
#define OMNISTREAM_TUPLETYPEINFO_H


#include "../../core/typeutils/TypeSerializer.h"
#include "../../core/typeinfo/TypeInformation.h"

#include <nlohmann/json.hpp>

using json = nlohmann::json;

class TupleTypeInfo : public TypeInformation {
public:
    explicit TupleTypeInfo(TypeSerializer *typeSerializer) :
            typeSerializer(typeSerializer) {
    };

    TypeSerializer *createTypeSerializer(std::string) override;

    std::string name() override;

    ~TupleTypeInfo() override = default;

    static TupleTypeInfo *of(const json &type);

    BackendDataType getBackendId() const override;

private:
    TypeSerializer *typeSerializer;
};


#endif //OMNISTREAM_TUPLETYPEINFO_H
