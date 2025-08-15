#ifndef FLINK_TNEL_COUNTFUNCTION_H
#define FLINK_TNEL_COUNTFUNCTION_H

#include "../AggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"
using namespace omniruntime::type;
class CountFunction : public AggsHandleFunction {
private:
    long aggCount;
    bool valueIsNull;
    int aggIdx;
    int accIndex;
    int valueIndex;
    int filterIndex;
    bool hasFilter;
    bool isCountStar;
    omniruntime::type::DataTypeId typeId;
    StateDataViewStore *store;

public:
    CountFunction(int aggIdx, std::string inputType, int accIndex, int valueIndex, int filterIndex = -1)
        : valueIsNull(true), aggIdx(aggIdx), accIndex(accIndex), valueIndex(valueIndex), filterIndex(filterIndex)
    {
        typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
        hasFilter = filterIndex != -1;
        isCountStar = false;
    }

    void setWindowSize(int windowSize) override {};
    bool equaliser(BinaryRowData *r1, BinaryRowData *r2) override;
    void open(StateDataViewStore *store);
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
    void setCountStart(bool isCountStartFunc);
    // RuntimeContext* getRuntimeContext();
};


#endif // FLINK_TNEL_COUNTFUNCTION_H
