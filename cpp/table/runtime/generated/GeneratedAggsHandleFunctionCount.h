#ifndef GENERATED_AGGS_HANDLER_FUNCTION_COUNT_H
#define GENERATED_AGGS_HANDLER_FUNCTION_COUNT_H

#include "AggsHandleFunction.h"
#include "table/runtime/dataview/StateDataViewStore.h"
#include "functions/RuntimeContext.h"

// TODO: getRuntimeContext makes store call its own getRuntimeContext which doesnt exist.
class GeneratedAggsHandleFunctionCount : public AggsHandleFunction {
private:
    long aggCount;
    bool valueIsNull;
    int aggIdx;
    int accIndex;
    int valueIndex;
    StateDataViewStore* store;

public:

    GeneratedAggsHandleFunctionCount(int aggIdx, int accIndex, int valueIndex) :
        valueIsNull(true), 
        aggIdx(aggIdx),
        accIndex(accIndex),
        valueIndex(valueIndex){}

    void setWindowSize(int windowSize) override {};
    bool equaliser(BinaryRowData* r1, BinaryRowData* r2) override;
    void open(StateDataViewStore* store);
    void accumulate(RowData* accInput) override;
    void accumulate(omnistream::VectorBatch *input, const std::vector<int> &indices) override;
    void retract(RowData* retractInput) override;
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void merge(RowData* otherAcc) override;
    void setAccumulators(RowData* acc) override;
    void resetAccumulators() override;
    void getAccumulators(BinaryRowData* accumulators) override;
    void createAccumulators(BinaryRowData* accumulators) override;
    void getValue(BinaryRowData* aggValue) override;
    void cleanup() override {};
    void close() override {};
    // RuntimeContext* getRuntimeContext();

};

#endif // GENERATED_AGGS_HANDLER_FUNCTION_COUNT_H
