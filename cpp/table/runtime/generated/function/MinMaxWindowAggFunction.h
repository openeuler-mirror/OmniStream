#ifndef MINWINDOWAGGFUNCTION_H
#define MINWINDOWAGGFUNCTION_H

#include <optional>
#include <stdexcept>
#include <cstdint>
#include <memory>

#include "MinMaxFunction.h"
#include "table/data/GenericRowData.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"


class MinMaxWindowAggFunction : public NamespaceAggsHandleFunction<int64_t> {
private:
    //todo: slicesharedSliceAssigner hop and cumulative
    StateDataViewStore* store;

    int64_t namespaceVal;
    bool valueIsNull;
    int aggIdx;
    int accIndex;
    int valueIndex;
    FuncType aggOperator;
    SliceAssigner* sliceAssigner; // tumble
    long aggValue;
    long limit;
    long aggCountValue;
    bool countIsNull;
    const int countIdx = 1;

public:
    // todo sharedassigner
    MinMaxWindowAggFunction(int aggIdx, int accIndex, int valueIndex, FuncType aggOperator, SliceAssigner* sliceAssigner)
        :aggIdx(aggIdx),
        accIndex(accIndex),
        valueIndex(valueIndex),
        aggOperator(aggOperator),
        sliceAssigner(sliceAssigner),
        aggValue(aggOperator == MAX_FUNC ? std::numeric_limits<long>::min() : std::numeric_limits<long>::max()),
        limit(aggOperator == MAX_FUNC ? std::numeric_limits<long>::min() : std::numeric_limits<long>::max()) {

    };
    void open(StateDataViewStore* store) override;
    void accumulate(RowData *accInput) override;
    void merge(long ns, RowData *otherAcc) override;
    void setAccumulators(long ns, RowData *acc) override;
    RowData *getAccumulators() override;
    RowData *createAccumulators(int accumulatorArity) override;
    RowData *getValue(long ns) override;
    void retract(RowData *input) override;
    void Cleanup(long ns) override;
    void close() override;
};



#endif //MINWINDOWAGGFUNCTION_H