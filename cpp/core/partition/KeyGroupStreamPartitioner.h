/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef KEYGROUPSTREAMPARTITIONER_H
#define KEYGROUPSTREAMPARTITIONER_H
#include <nlohmann/json.hpp>
#include <vector>
#include "StreamPartitioner.h"
#include "table/data/binary/BinaryRowData.h"
#include "../io/IOReadableWritable.h"
#include "table/data/writer/BinaryRowWriter.h"
#include "udf/UDFLoader.h"
#include "functions/KeySelect.h"
#include "basictypes/Tuple2.h"
#include "../streamrecord/StreamRecord.h"
#include "../plugable/SerializationDelegate.h"
#include "table/data/binary/MurmurHashUtils.h"
#include "../util/MathUtils.h"
#include "runtime/state/KeyGroupRangeAssignment.h"

using json = nlohmann::json;

namespace omnistream::datastream {
    template<typename T, typename K>
    class KeyGroupStreamPartitioner : public StreamPartitioner<T> {
    public:
        KeyGroupStreamPartitioner(nlohmann::json config, int targetId, int maxParallelism)
            : config(config), targetId(targetId), maxParallelism(maxParallelism)
        {
            std::string udfObj = config["udf_obj"];
            std::string keySelectorPath = config["hash_path"];
            std::string keySelectorName = config["hash_so"][std::to_string(targetId)];
            std::string path = keySelectorPath + keySelectorName;

            nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);
            auto symbol = udfLoader.LoadKeySelectFunction(path);
            keySelector = symbol(udfObjJson);
            if (maxParallelism <= 0) {
                throw std::invalid_argument("Number of key-groups must be > 0!");
            }
            if (!keySelector) {
                throw std::invalid_argument("Key selector cannot be null");
            }
        }

        int getMaxParallelism() const
        {
            return maxParallelism;
        }

        int selectChannel(T* record) override
        {
            K* key;
            try {
                SerializationDelegate *serializationDelegate = reinterpret_cast<SerializationDelegate *>(record);
                StreamRecord *streamRecord = reinterpret_cast<StreamRecord *>(serializationDelegate->getInstance());
                // getkey() function maybe call getPutCount().
                key = keySelector->getKey(static_cast<K*>(streamRecord->getValue()));
                static_cast<Object*>(key)->putRefCount();
            } catch (const std::exception& e) {
                throw std::runtime_error("Could not extract key from ");
            }
            return KeyGroupRangeAssignment<K*>::assignKeyToParallelOperator(key, maxParallelism, this->numberOfChannels);
        }

        std::unique_ptr<StreamPartitioner<T>> copy() override
        {
            return std::make_unique<KeyGroupStreamPartitioner<T, K>>(config, targetId, maxParallelism);
        }

        bool isPointWise() const override
        {
            return false;
        }

        [[nodiscard]] std::string toString() const override
        {
            return "HASH";
        }

        void configure(int newMaxParallelism)
        {
            this->maxParallelism = newMaxParallelism;
        }
    private:
        UDFLoader udfLoader;
        nlohmann::json config;
        int targetId;
        KeySelectUnique<K> keySelector = nullptr;
        int maxParallelism;
    };
}
#endif //KEYGROUPSTREAMPARTITIONER_H
