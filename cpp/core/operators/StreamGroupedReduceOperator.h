/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNIFLINK_STREAMGROUPEDREDUCEOPERATOR_H
#define OMNIFLINK_STREAMGROUPEDREDUCEOPERATOR_H

#include <filesystem>
#include "AbstractUdfStreamOperator.h"
#include "OneInputStreamOperator.h"
#include "../streamrecord/StreamRecord.h"
#include "functions/ReduceFunction.h"
#include "udf/UDFLoader.h"
#include "core/typeutils/TypeSerializerConstants.h"
#include "core/typeutils/TupleSerializer.h"
#include "core/typeutils/ObjectSerializer.h"
#include "connector-kafka/bind_core_manager.h"

namespace omnistream::datastream {
    template<typename K>
    class StreamGroupedReduceOperator
            : public AbstractUdfStreamOperator<ReduceFunction<K>, K*>, public OneInputStreamOperator {
    public:
        explicit StreamGroupedReduceOperator(Output *output, nlohmann::json config)
        {
            LOG("-----create StreamGroupedReduceOperator-----");
            this->output = output;
            loadUdf(config);
            this->userFunction = function.get();
        }

        explicit StreamGroupedReduceOperator(Output *output)
        {
            this->output = output;
        }

        void loadUdf(const nlohmann::json &config)
        {
            std::string udfSoPath = config["udf_so"];
            std::string KeySoPath = config["key_so"];
            std::string udfObj = config["udf_obj"];

            nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);

            auto *symbol = udfLoader.LoadReduceFunction(udfSoPath);
            if (symbol == nullptr) {
                throw std::out_of_range("null pointer when load " + udfSoPath);
            }
            function = symbol(udfObjJson);

            auto *keySelectorSymbol = udfLoader.LoadKeySelectFunction(KeySoPath);
            if (keySelectorSymbol == nullptr) {
                throw std::out_of_range("null pointer when load " + KeySoPath);
            }
            keySelector = keySelectorSymbol(udfObjJson);
            this->userFunction = function.get();
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

            auto currentValue = reinterpret_cast<Object *>(values->value());
            if (currentValue != nullptr) {
                Object *out = function->reduce(currentValue, input);
//                auto backend = out->clone();
                record->setValue(out);
                currentValue->putRefCount();
                out->getRefCount();
                values->update(out);
                this->output->collect(record);
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
            auto taskId = initializer->getEnvironment()->getTaskInfo()->getIndexOfThisSubtask();
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
            auto stateId = new ValueStateDescriptor(STATE_NAME, new ObjectSerializer());
            auto keyedStateStore = this->stateHandler->getKeyedStateStore();
            values = keyedStateStore->template getState<K*>(stateId);
        }

        bool canBeStreamOperator() override
        {
            return true;
        }

        void close() override {
        }

        void processWatermarkStatus(WatermarkStatus *watermarkStatus) override
        {
            NOT_IMPL_EXCEPTION;
        }

    private:
        std::string STATE_NAME = "_op_state";
        ValueState<K*> *values;
        TypeSerializer *serializer;
        UDFLoader udfLoader;
        ReduceFunctionUnique<K> function;
        KeySelectUnique<K> keySelector;
        int32_t coreId = -1;
        bool binded = false;
    };
}

#endif  //OMNIFLINK_STREAMGROUPEDREDUCEOPERATOR_H
