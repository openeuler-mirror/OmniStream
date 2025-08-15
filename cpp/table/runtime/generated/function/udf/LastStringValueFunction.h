/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

#ifndef OMNISTREAM_LASTSTRINGVALUEFUNCTION_H
#define OMNISTREAM_LASTSTRINGVALUEFUNCTION_H

#include "runtime/generated/AggsHandleFunction.h"

using namespace omniruntime::type;
class LastStringValueFunction : public AggsHandleFunction {
public:
    LastStringValueFunction(int aggIdx, std::string inputType, int accIndex, int valueIndex)
        : valueIsNull(true), aggIdx(aggIdx), accIndex(accIndex), valueIndex(valueIndex)
    {
        typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
        emptyStringAggValue = "";
    }

    void setWindowSize(int windowSize) override {};
    bool equaliser(BinaryRowData *r1, BinaryRowData *r2) override;
    void accumulate(RowData *accInput) override;
    void accumulate(omnistream::VectorBatch *input, const std::vector<int>& indices) override;
    void retract(RowData *retractInput) override;
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void merge(RowData *otherAcc) override;
    void setAccumulators(RowData *acc) override;
    void resetAccumulators() override;
    void getAccumulators(BinaryRowData *accumulators) override;
    void createAccumulators(BinaryRowData *accumulators) override;
    void getValue(BinaryRowData *aggValue) override;
    void cleanup() override {};
    void close() override {};

private:
    bool valueIsNull;
    int aggIdx;
    int accIndex;
    int valueIndex;
    std::string stringAggValue;
    omniruntime::type::DataTypeId typeId;
    std::string emptyStringAggValue;
};


#endif // OMNISTREAM_LASTSTRINGVALUEFUNCTION_H
