//
// Created by f30029561 on 2025/2/11.
//

#ifndef FLINK_TNEL_AVERAGEFUNCTION_H
#define FLINK_TNEL_AVERAGEFUNCTION_H

#include "../AggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"
using namespace omniruntime::type;
class AverageFunction : public AggsHandleFunction {
private:
    long sum = 0;
    bool sumIsNull = false;
    long count0 = 0;
    bool count0IsNull = false;
//    long count1 = 0;
//    bool count1IsNull = false;
    int aggIdx;
    omniruntime::type::DataTypeId typeId;
    int accIndexSum;

    int accIndexCount0;
//    int accIndexCount1;
    int valueIndex;
    int filterIndex;
    bool hasFilter;
    StateDataViewStore *store;

public:
    AverageFunction(int aggIdx, std::string inputType, int accIndexSum, int accIndexCount0, int valueIndex, int filterIndex = -1);
    void accumulate(RowData *accInput) override;
    void accumulate(omnistream::VectorBatch *input, const std::vector<int>& indices) override;
    void retract(RowData *retractInput) override;
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void merge(RowData *otherAcc) override;
    void setAccumulators(RowData *_acc) override;
    void createAccumulators(BinaryRowData* accumulators) override;
    void resetAccumulators() override;
    void open(StateDataViewStore *store);
    void setWindowSize(int windowSize) {}
    void cleanup() override {}
    void close() override {}
    void getAccumulators(BinaryRowData *accumulators) override;
    void getValue(BinaryRowData *aggValue) override;
    bool equaliser(BinaryRowData *r1, BinaryRowData *r2) override;
};


#endif // FLINK_TNEL_AVERAGEFUNCTION_H
