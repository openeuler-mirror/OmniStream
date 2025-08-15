//
// Created by f30029561 on 2025/2/12.
//

#ifndef FLINK_TNEL_SUMFUNCTION_H
#define FLINK_TNEL_SUMFUNCTION_H

#include "../AggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"
using namespace omniruntime::type;
class SumFunction : public AggsHandleFunction {
private:
    long sum = 0;
    long count0 = 0;
    bool sumIsNull = false;
    bool count0IsNull = false;
    bool consumeRetraction = false;
    int aggIdx;
    std::string inputType;
    int accIndex;
    int accIndexCount0;
    int valueIndex;
    int filterIndex;
    bool hasFilter;
    StateDataViewStore *store;

public:
    SumFunction(int aggIdx, std::string inputType, int accIndex, int valueIndex, int filterIndex = -1);
    void accumulate(RowData *accInput) override;
    void accumulate(omnistream::VectorBatch *input, const std::vector<int> &indices) override;
    void retract(RowData *retractInput) override;
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void merge(RowData *otherAcc) override;
    void setAccumulators(RowData *_acc) override;
    void resetAccumulators() override;
    void createAccumulators(BinaryRowData* accumulators) override;
    void open(StateDataViewStore *store);
    void setWindowSize(int windowSize) {}
    void cleanup() override {}
    void close() override {}
    void getAccumulators(BinaryRowData *accumulators) override;
    void getValue(BinaryRowData *aggValue) override;
    bool equaliser(BinaryRowData *r1, BinaryRowData *r2) override;
    void setRetraction(int accIndexCount0Index);
};


#endif // FLINK_TNEL_SUMFUNCTION_H