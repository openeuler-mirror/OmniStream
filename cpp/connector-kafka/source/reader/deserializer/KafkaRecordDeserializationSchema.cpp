/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
*/

#include "KafkaRecordDeserializationSchema.h"
#include "KafkaValueOnlyDeserializationSchemaWrapper.h"

KafkaRecordDeserializationSchema* KafkaRecordDeserializationSchema::valueOnly(
    DeserializationSchema* valueDeserializationSchema)
{
    return new KafkaValueOnlyDeserializationSchemaWrapper(valueDeserializationSchema);
}
