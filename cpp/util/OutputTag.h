/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by xichen on 2/5/25.
//

#ifndef FLINK_TNEL_OUTPUTTAG_H
#define FLINK_TNEL_OUTPUTTAG_H

#include <string>
#include "core/typeinfo/TypeInformation.h"

class OutputTag {
public:
    // OutputTag(std::string &id) : id(std::move(id)) {}

    OutputTag(std::string &id, TypeInformation *typeInfo) : id(std::move(id)), typeInfo(typeInfo) {}

    std::string getId() const
    {
        return id;
    }
    TypeInformation *getTypeInformation() const
    {
        return typeInfo;
    }
    bool isResponsibleFor(OutputTag* owner, OutputTag* other) const
    {
        return owner->equals(*other);
    }
    bool equals(OutputTag& other)
    {
        return other.id == this->id;
    }
private:
    std::string id;
    TypeInformation *typeInfo;
};
#endif //FLINK_TNEL_OUTPUTTAG_H
