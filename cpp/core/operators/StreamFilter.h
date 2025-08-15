/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNIFLINK_STREAMFILTER_H
#define OMNIFLINK_STREAMFILTER_H

#include <task/ChainingOutput.h>
#include <task/DataStreamChainingOutput.h>

#include "AbstractUdfStreamOperator.h"
#include "functions/FilterFunction.h"
#include "OneInputStreamOperator.h"
#include "core/udf/UDFLoader.h"

namespace omnistream::datastream {
    template<typename F, typename K>
    class StreamFilter : public AbstractUdfStreamOperator<FilterFunction<F>, K>,
                         public OneInputStreamOperator {
    public:
        StreamFilter(Output *output, nlohmann::json config)
        {
            LOG("-----create StreamFilter-----");
            std::string soPath = config["udf_so"];
            std::string udfObj = config["udf_obj"];
            nlohmann::json udfObjJson = nlohmann::json::parse(udfObj);
            std::cout << "filter udf obj: " << udfObj << std::endl;
            auto *symbol = udfLoader.LoadFilterFunction(soPath);
            if (symbol == nullptr) {
                throw std::out_of_range("null pointer when load " + soPath);
            }

            udfFilterfunction = symbol(udfObjJson);
            this->userFunction = udfFilterfunction.get();
            isStream = true;
            auto casted = dynamic_cast<DataStreamChainingOutput *>(output);
            if (casted != nullptr) {
                casted->disableFree();
            }
            this->setOutput(output);
        };

        StreamFilter(Output *output, BatchFilterFunction<F>* filterFunction)
        {
            LOG("-----create StreamFilter-----");
            this->setOutput(output);
            function = filterFunction;
            this->userFunction = filterFunction;
        };

        void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
        {
            // do nothing
        }

        void processElement(StreamRecord *record) override
        {
            Object *value = reinterpret_cast<Object *>(record->getValue());
            if (this->udfFilterfunction->filter(value)) {
                this->output->collect(record);
            } else {
                value->putRefCount();
            }
        }

        void processBatch(StreamRecord *record) override
        {
            auto newBatch = function->filterBatch(record->getValue());
            if (newBatch.empty()) {
                return;
            }
            auto *newVectorBatch = new omnistream::VectorBatch(newBatch.size());
            auto *input = reinterpret_cast<omnistream::VectorBatch*>(record->getValue());
            int32_t colSize = input->GetVectorCount();
            auto colVecs = input->GetVectors();
            for (int32_t i = 0; i < colSize; i++) {
                applyFilterResult(newBatch, newVectorBatch, colVecs, i);
            }
            omniruntime::vec::VectorHelper::FreeVecBatch(input);
            record->setValue(newVectorBatch);
            AbstractStreamOperator<K>::output->collect(record);
        }

        void applyFilterResult(const std::vector<int> &newBatch, VectorBatch *newVectorBatch,
            omniruntime::vec::BaseVector *const *colVecs, int32_t i) const
        {
            switch (colVecs[i]->GetTypeId()) {
                case (OMNI_LONG) : {
                    auto oldVec = reinterpret_cast<omniruntime::vec::Vector<int64_t> *>(colVecs[i]);
                    auto vec = new omniruntime::vec::Vector<int64_t>(newBatch.size());
                    for (size_t j = 0; j < newBatch.size(); j++) {
                        vec->SetValue(j, oldVec->GetValue(newBatch[j]));
                    }
                    newVectorBatch->Append(vec);
                    break;
                }
                case (OMNI_CHAR):
                case (OMNI_VARCHAR) : {
                    auto oldVec = reinterpret_cast<omniruntime::vec::Vector<
                        omniruntime::vec::LargeStringContainer<std::string_view>> *>(colVecs[i]);
                    auto vec = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(
                        newBatch.size());
                    for (size_t j = 0; j < newBatch.size(); j++) {
                        auto val = oldVec->GetValue(newBatch[j]);
                        vec->SetValue(j, val);
                    }
                    newVectorBatch->Append(vec);
                    break;
                }
                case (OMNI_BOOLEAN) : {
                    auto oldVec = reinterpret_cast<omniruntime::vec::Vector<bool> *>(colVecs[i]);
                    auto vec = new omniruntime::vec::Vector<bool>(newBatch.size());
                    for (size_t j = 0; j < newBatch.size(); j++) {
                        vec->SetValue(j, oldVec->GetValue(newBatch[j]));
                    }
                    newVectorBatch->Append(vec);
                    break;
                }
                default:
                    throw std::runtime_error("Unsupported type: ");
            }
        }

        void processWatermarkStatus(WatermarkStatus *watermarkStatus) override {
            NOT_IMPL_EXCEPTION;
        }

        bool canBeStreamOperator() override {
            return isStream;
        }

        void open() override {
        }

        void close() override {
        }

        const char *getName() override
        {
            return "StreamFilter";
        }

        std::string getTypeName() override
        {
            std::string typeName = "StreamFilter";
            typeName.append(__PRETTY_FUNCTION__) ;
            return typeName ;
        }

    private:
        BatchFilterFunction<F>* function;
        UDFLoader udfLoader;
        FilterFunctionUnique<F> udfFilterfunction;
        bool isStream = false;
    };
}
#endif   // OMNIFLINK_STREAMFILTER_H
