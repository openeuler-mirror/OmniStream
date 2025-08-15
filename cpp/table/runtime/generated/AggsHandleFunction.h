#ifndef FLINK_TNEL_AGGS_HANDLE_FUNCTION_H
#define FLINK_TNEL_AGGS_HANDLE_FUNCTION_H

#include "AggsHandleFunctionBase.h"
#include "../../data/binary/BinaryRowData.h"
#include "table/vectorbatch/VectorBatch.h"

class AggsHandleFunction {
public:
    virtual ~AggsHandleFunction() = default;

    virtual void getValue(BinaryRowData* aggValue) = 0;

    virtual void setWindowSize(int windowSize) = 0;

    virtual bool equaliser(BinaryRowData* r1, BinaryRowData* r2) = 0;
    virtual void accumulate(RowData* input) = 0;
    virtual void accumulate(omnistream::VectorBatch *input, const std::vector<int>& indices) = 0;
    virtual void retract(RowData* input) = 0;
    virtual void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) = 0;
    virtual void merge(RowData* accumulators) = 0;
    virtual void createAccumulators(BinaryRowData* accumulators) = 0;
    virtual void setAccumulators(RowData* accumulators) = 0;
    virtual void resetAccumulators() = 0;
    virtual void getAccumulators(BinaryRowData* accumulators) = 0;
    virtual void cleanup() = 0;
    virtual void close() = 0;
};

#endif // FLINK_TNEL_AGGS_HANDLE_FUNCTION_H