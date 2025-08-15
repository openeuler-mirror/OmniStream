/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef VOIDTYPEINFO_H
#define VOIDTYPEINFO_H

#include "../typeutils/VoidSerializer.h"
#include "TypeInformation.h"

class VoidTypeInfo : public TypeInformation {
public:
    explicit VoidTypeInfo(const char* name);
    TypeSerializer* createTypeSerializer(const std::string config) override;
    std::string name() override;
    BackendDataType getBackendId() const override {return BackendDataType::VOID_NAMESPACE_BK;}

private:
    std::string name_;
};

#endif  // VOIDTYPEINFO_H