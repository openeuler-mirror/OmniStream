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

#ifndef OMNIFLINK_STREAMGROUPEDREDUCEOPERATOR_H
#define OMNIFLINK_STREAMGROUPEDREDUCEOPERATOR_H

#include <filesystem>
#include "AbstractUdfStreamOperator.h"
#include "OneInputStreamOperator.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "functions/ReduceFunction.h"
#include "udf/UDFLoader.h"
#include "core/typeutils/TypeSerializerConstants.h"
#include "core/typeutils/TupleSerializer.h"
#include "core/typeutils/ObjectSerializer.h"
#include "connector/kafka/bind_core_manager.h"
/**
 * K: Object
 * */
namespace omnistream::datastream {
    template<typename K>
    class StreamGroupedReduceOperator
            : public AbstractUdfStreamOperator<ReduceFunction<K>, K*>, public OneInputStreamOperator {
    public:
        explicit StreamGroupedReduceOperator(Output *output, nlohmann::json config, bool isStream = true)
        {
            LOG("-----create StreamGroupedReduceOperator-----");
            this->output = output;
            this->isStream = isStream;
            loadUdf(config);
        }

        // only for test
        explicit StreamGroupedReduceOperator(Output *output, bool isStream = true)
        {
            this->output = output;
            this->isStream = isStream;
        }

        void loadUdf(const nlohmann::json &config)
        {
            std::string udfSoPath = config["udf_so"];
            std::string keySoDir = config["hash_path"];
            std::string keySoName = config["key_so"][0];
            std::string udfObj = config["udf_obj"];

            nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);

            auto *symbol = udfLoader.LoadReduceFunction(udfSoPath);
            if (symbol == nullptr) {
                throw std::out_of_range("null pointer when load " + udfSoPath);
            }
            this->userFunction = symbol(udfObjJson).release();

            std::string path = keySoDir + keySoName;
            auto *keySelectorSymbol = udfLoader.LoadKeySelectFunction(path);
            if (keySelectorSymbol == nullptr) {
                throw std::out_of_range("null pointer when load " + keySoName);
            }
            keySelector = keySelectorSymbol(udfObjJson);
        }

        ~StreamGroupedReduceOperator() override {
        }

        void processElement(StreamRecord *record) override
        {
            if (unlikely(not binded)) {
                if (coreId >= 0) {
                    omnistream::BindCoreManager::GetInstance()->BindDirectCore(coreId);
                }
                binded = true;
            }
            LOG("-----StreamGroupedReduceOperator processElement start-----");
            Object *input = reinterpret_cast<Object *>(record->getValue());

            auto currentValue = reinterpret_cast<Object *>(values->value()); // currentValue already do getRefCount() in inner, so it need putRefCount later
            if (currentValue != nullptr) {
                Object *out = this->userFunction->reduce(currentValue, input);
                record->setValue(out);
                currentValue->putRefCount();
                values->update(out); // out already do getRefCount() in inner
                this->output->collect(record);
                input->putRefCount();
            } else {
                auto backend = input->clone();
                values->update(backend);

                this->output->collect(record);
            }
            LOG("-----StreamGroupedReduceOperator processElement end-----");
        }

        void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
        {
            LOG("-----StreamGroupedReduceOperator::initializeState")
            AbstractStreamOperator<K*>::initializeState(initializer, keySerializer);
            auto taskId = initializer->getEnvironment()->taskConfiguration().getIndexOfSubtask();
            auto& instance = omnistream::BindCoreManager::GetInstance();
            if (instance->NeedBindOp()) {
                coreId = instance->GetOpCore(taskId);
            }
        }

        bool isSetKeyContextElement() override
        {
            return true;
        }

        void setKeyContextElement(StreamRecord *record) override
        {
            Object *key = reinterpret_cast<Object*>(keySelector->getKey(reinterpret_cast<Object*>(record->getValue())));
            if (!key) {
                INFO_RELEASE("key in setKeyContextElement is null")
                throw std::runtime_error("key in setKeyContextElement is null");
            }
            this->stateHandler->setCurrentKey(key);
            key->putRefCount();
        }

        const char *getName() override
        {
            // NOT IMPLEMENTED
            return "StreamGroupedReduceOperator";
        }

        void open() override
        {
            AbstractUdfStreamOperator<ReduceFunction<K>, K*>::open();
            auto stateId = new ValueStateDescriptor<K*>(STATE_NAME, new ObjectSerializer());
            auto keyedStateStore = this->stateHandler->getKeyedStateStore();
            values = keyedStateStore->template getState<K*>(stateId);
        }

        void close() override {
        }

        void processWatermarkStatus(WatermarkStatus *watermarkStatus) override
        {
            NOT_IMPL_EXCEPTION;
        }

        bool canBeStreamOperator() override
        {
            return this->isStream;
        }

    private:
        std::string STATE_NAME = "_op_state";
        ValueState<K*> *values;
        TypeSerializer *serializer;
        UDFLoader udfLoader;
        KeySelectUnique<K> keySelector;
        int32_t coreId = -1;
        bool binded = false;
    };
}

#endif
