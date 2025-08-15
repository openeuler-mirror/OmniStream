/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNIFLINK_STREAMMAP_H
#define OMNIFLINK_STREAMMAP_H

#include "AbstractUdfStreamOperator.h"
#include "functions/MapFunction.h"
#include "OneInputStreamOperator.h"
#include "../streamrecord/StreamRecord.h"
#include "udf/UDFLoader.h"
#include "connector-kafka/bind_core_manager.h"
namespace omnistream::datastream {
    template<typename F, typename K>
    class StreamMap : public AbstractUdfStreamOperator<MapFunction<F>, K>, public OneInputStreamOperator {
    public:
        explicit StreamMap(Output *output, nlohmann::json config)
        {
            LOG("-----create StreamMap-----");
            this->output = output;
            loadUdf(config);
        };

        explicit StreamMap(Output *output)
        {
            this->output = output;
        }

        void loadUdf(const nlohmann::json &config)
        {
            std::string soPath = config["udf_so"];
            std::string udfObj = config["udf_obj"];
            nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);
            std::cout << "map udf obj: " << udfObj << std::endl;

            auto *symbol = udfLoader.LoadMapFunction(soPath);
            if (symbol == nullptr) {
                throw std::out_of_range("null pointer when load " + soPath);
            }
            function = symbol(udfObjJson);
            this->userFunction = function.get();
        }

        void processElement(StreamRecord *record) override
        {
            if (unlikely(not binded)) {
                if (coreId >= 0) {
                    omnistream::BindCoreManager::GetInstance()->BindDirectCore(coreId);
                }
                binded = true;
            }
            LOG("-----StreamMap processElement start -----");
            Object *out = function->map(reinterpret_cast<Object *>(record->getValue()));
            record->setValue(out);
            this->output->collect(record);
            LOG("-----StreamMap processElement end -----");
        };

        void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
        {
            auto taskId = initializer->getEnvironment()->getTaskInfo()->getIndexOfThisSubtask();
            auto & bindCore = omnistream::BindCoreManager::GetInstance();
            if (bindCore->NeedBindOp()) {
                coreId = omnistream::BindCoreManager::GetInstance()->GetOpCore(taskId);
            }
        }

        void processWatermarkStatus(WatermarkStatus *watermarkStatus) override
        {
            NOT_IMPL_EXCEPTION;
        }

        const char *getName() override
        {
            // NOT IMPLEMENTED
            return "StreamMap";
        }

        void open() override {
        }

        bool canBeStreamOperator() override
        {
            return true;
        }

        void close() override {
        }

        Output* getOutput ()
        {
            return this->output;
        }
    private:
        UDFLoader udfLoader;
        MapFunctionUnique<F> function;
        int32_t coreId = -1;
        bool binded = false;
    };
}
#endif  //OMNIFLINK_STREAMMAP_H
