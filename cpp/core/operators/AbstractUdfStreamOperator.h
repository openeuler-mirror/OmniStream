/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_ABSTRACTUDFSTREAMOPERATOR_H
#define FLINK_TNEL_ABSTRACTUDFSTREAMOPERATOR_H

#include "AbstractStreamOperator.h"
#include "streaming/api/functions/KeyedProcessFunction.h"

// Class should also contain the following if needed
// <OUT, F extends Function>
// OUT is the output type of the operator, the DataView requires it
// F is the UDF initialized through codegen
template <typename F, typename K>
class AbstractUdfStreamOperator : public AbstractStreamOperator<K>
{
public:
    AbstractUdfStreamOperator() = default;
    explicit AbstractUdfStreamOperator(F* userFunction) : userFunction(userFunction) {}
    AbstractUdfStreamOperator(F* userFunction, Output* output) : AbstractStreamOperator<K>(output), userFunction(userFunction) {}

    ~AbstractUdfStreamOperator()
    {
        if constexpr (std::is_base_of_v<RichFunction, F>) {
            delete userFunction;
        }
    };

    F* getUserFunction() { return userFunction; }
    void open() override {
        AbstractStreamOperator<K>::open();
        auto richFunctionPtr = dynamic_cast<RichFunction*>(userFunction);
        if (richFunctionPtr != nullptr) {
            richFunctionPtr->setRuntimeContext(this->runtimeContext);
            richFunctionPtr->open(Configuration());
        }
    }

    std::string getTypeName() override {
        std::string typeName = "AbstractUdfStreamOperator";
        typeName.append(__PRETTY_FUNCTION__) ;
        return typeName ;
    }
protected:
    F* userFunction = nullptr;
};

#endif // FLINK_TNEL_ABSTRACTUDFSTREAMOPERATOR_H
