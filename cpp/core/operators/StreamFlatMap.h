/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_STREAMFLATMAP1_H
#define FLINK_TNEL_STREAMFLATMAP1_H

#include "AbstractUdfStreamOperator.h"
#include "functions/FlatMapFunction.h"
#include "OneInputStreamOperator.h"
#include "TimestampedCollector.h"
#include "udf/UDFLoader.h"
namespace omnistream::datastream {
    template<typename F, typename K>
    class StreamFlatMap
            : public AbstractUdfStreamOperator<FlatMapFunction<F>, K>, public OneInputStreamOperator {
    public:
        StreamFlatMap(Output *output, nlohmann::json config) {
            LOG("-----create StreamFlatMap-----");
            std::string soPath = config["udf_so"];
            std::string udfObj = config["udf_obj"];
            nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);
            std::cout << "flatmap udf obj: " << udfObj << std::endl;
            auto *symbol = udfLoader.LoadFlatMapFunction(soPath);
            if (symbol == nullptr) {
                throw std::out_of_range("null pointer when load " + soPath);
            }
            function = symbol(udfObjJson);
            this->userFunction = function.get();

            this->setOutput(output);
        }

        ~StreamFlatMap() override {
            delete collector;
        }

        void open() override {
            AbstractUdfStreamOperator<FlatMapFunction<F>, K>::open();
            collector = new TimestampedCollector(this->output, true);
        }

        void processElement(StreamRecord *record) override {
            collector->setTimestamp(record);
            function->flatMap(reinterpret_cast<Object *>(record->getValue()), collector);
        }

        void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override {
            // Do Nothing
        }

        void processWatermarkStatus(WatermarkStatus *watermarkStatus) override {
            NOT_IMPL_EXCEPTION;
        }

        bool canBeStreamOperator() override {
            return true;
        }

        const char *getName() override {
            return "StreamFlatMap";
        }

    private:
        UDFLoader udfLoader;
        FlatMapFunctionUnique<F> function;
        TimestampedCollector *collector;
    };
}

#endif   //FLINK_TNEL_STREAMFLATMAP1_H