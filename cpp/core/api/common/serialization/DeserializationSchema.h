/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_DESERIALIZATIONSCHEMA_H
#define FLINK_TNEL_DESERIALIZATIONSCHEMA_H


#include <vector>
#include <memory>
#include <stdexcept>
#include "functions/Collector.h"
#include "core/typeinfo/TypeInformation.h"
#include "vectorbatch/VectorBatch.h"

class DeserializationSchema {
public:
    virtual ~DeserializationSchema() = default;

    virtual void Open() {}

    virtual Object* deserialize(const uint8_t* message, size_t length)
    {
        NOT_IMPL_EXCEPTION;
    }

    virtual void* deserialize(std::vector<const uint8_t*>& messageVec, std::vector<size_t>& lengthVec)
    {
        NOT_IMPL_EXCEPTION;
    }

    virtual void deserialize(const uint8_t* message, size_t length, Collector* out)
    {
        auto deserialized = deserialize(message, length);
        if (deserialized != nullptr) {
            out->collect(deserialized);
        }
    }

    virtual void deserialize(std::vector<const uint8_t*>& messageVec, std::vector<size_t>& lengthVec,
        std::vector<int64_t>& timeVec, Collector* out)
    {
        auto deserialized = deserialize(messageVec, lengthVec);
        if (deserialized == nullptr) {
            return;
        }
        auto* vectorBatch = reinterpret_cast<omnistream::VectorBatch*>(deserialized);
        if (timeVec.size() == messageVec.size()) {
            for (size_t rowIndex = 0; rowIndex < messageVec.size(); rowIndex++) {
                vectorBatch->setTimestamp(rowIndex, timeVec[rowIndex]);
            }
        }
        out->collect(deserialized);
    }

    virtual bool isEndOfStream(const void* nextElement) = 0;

    virtual omnistream::VectorBatch* createBatch(int size, std::vector<DataTypeId>& typeVec)
    {
        auto *vectorBatch = new omnistream::VectorBatch(size);
        for (auto& type : typeVec) {
            switch (type) {
                case (omniruntime::type::DataTypeId::OMNI_LONG):
                case (omniruntime::type::DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE):
                case (omniruntime::type::DataTypeId::OMNI_TIMESTAMP): {
                    auto vec = new omniruntime::vec::Vector<int64_t>(size);
                    vectorBatch->Append(vec);
                    break;
                }
                case (omniruntime::type::DataTypeId::OMNI_CHAR):
                case (omniruntime::type::DataTypeId::OMNI_VARCHAR) : {
                    auto vec =
                        new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(size);
                    vectorBatch->Append(vec);
                    break;
                }
                default:
                    throw std::runtime_error("Unsupported type: " + type);
            }
        }
        return vectorBatch;
    }
};

#endif // FLINK_TNEL_DESERIALIZATIONSCHEMA_H
