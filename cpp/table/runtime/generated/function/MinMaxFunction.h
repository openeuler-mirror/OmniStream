#ifndef FLINK_TNEL_MINMAXFUNCTION_H
#define FLINK_TNEL_MINMAXFUNCTION_H
#include "../AggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"

using namespace omniruntime::type;
enum FuncType {
    MAX_FUNC,
    MIN_FUNC
};
class MinMaxFunction : public AggsHandleFunction {
private:
    FuncType aggOperator;
    long aggValue;

    std::string stringAggValue;
    std::string stringAggValueLimit;
    bool valueIsNull;
    int aggIdx;
    int accIndex;
    int valueIndex;
    long limit;
    int filterIndex;
    bool hasFilter;
    omniruntime::type::DataTypeId typeId;

    StateDataViewStore *store = nullptr;

public:
    MinMaxFunction(int aggIdx, std::string inputType, int accIndex, int valueIndex, FuncType aggOperator, int filterIndex = -1)
        : aggOperator(aggOperator),
          aggValue(aggOperator == MAX_FUNC ? std::numeric_limits<long>::min() : std::numeric_limits<long>::max()),
          valueIsNull(true),
          aggIdx(aggIdx),
          accIndex(accIndex),
          valueIndex(valueIndex),
          limit(aggOperator == MAX_FUNC ? std::numeric_limits<long>::min() : std::numeric_limits<long>::max()),
          filterIndex(filterIndex)
    {
        typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
        hasFilter = filterIndex != -1;
    }

    void open(StateDataViewStore *store);
    void setWindowSize(int windowSize) override {};
    void accumulate(RowData *accInput) override;
    void accumulate(omnistream::VectorBatch *input, const std::vector<int> &indices) override;
    void accumulateLong(RowData *accInput);
    void accumulateLong(omniruntime::vec::BaseVector *columnData, int rowIndex);
    void accumulateString(RowData *accInput);
    void accumulateString(omniruntime::vec::BaseVector *columnData, int rowIndex);
    int compareStrRowDataSimple(const std::string& left, const std::string& right);
    void retract(RowData *retractInput) override;
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void merge(RowData *otherAcc) override;
    void setAccumulators(RowData *acc) override;
    void createAccumulators(BinaryRowData* accumulators) override;
    void resetAccumulators() override;
    void getAccumulators(BinaryRowData *accumulators) override;
    void getValue(BinaryRowData *aggValue) override;
    void cleanup() override {};
    void close() override {};
    bool equaliser(BinaryRowData *r1, BinaryRowData *r2) override;
};


#endif // FLINK_TNEL_MINMAXFUNCTION_H
