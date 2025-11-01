/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef FLINK_TNEL_JSONDESERIALIZATIONSCHEMA_H
#define FLINK_TNEL_JSONDESERIALIZATIONSCHEMA_H


#include <nlohmann/json.hpp>
#include "core/api/common/serialization/AbstractDeserializationSchema.h"
#include "datagen/meituan/OriginalRecord.h"

class JsonDeserializationSchema : public AbstractDeserializationSchema {
public:
    explicit JsonDeserializationSchema(std::string inputType);
    void Open() override;
private:
    OriginalRecord reuseRecord;
    nlohmann::json j;
};


#endif // FLINK_TNEL_JSONDESERIALIZATIONSCHEMA_H
