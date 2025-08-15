#ifndef COUNTWINDOWAGGFUNCTION_H
#define COUNTWINDOWAGGFUNCTION_H

#include <stdexcept>
#include <optional>
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "table/data/GenericRowData.h"
#include "table/runtime/dataview/StateDataViewStore.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "table/data/TimestampData.h"

class CountWindowAggFunction : public NamespaceAggsHandleFunction<int64_t> {
private:
    SliceAssigner* sliceAssigner; // tumble
    StateDataViewStore* store;

    int64_t namespaceVal;
    bool valueIsNull = false;
    int aggIdx;
    int accIndex;
    int valueIndex;
    long limit;
    long aggValue;

public:
    CountWindowAggFunction(int aggIdx, int accIndex, int valueIndex, SliceAssigner* sliceAssigner)
        :sliceAssigner(sliceAssigner),
        aggIdx(aggIdx),
        accIndex(accIndex),
        valueIndex(valueIndex){};
    void open(StateDataViewStore* store) override;
    void accumulate(RowData *accInput) override;
    void merge(long ns, RowData *otherAcc) override;
    void setAccumulators(long ns, RowData *acc) override;
    RowData *getAccumulators() override;
    RowData *createAccumulators(int accumulatorArity) override;
    RowData *getValue(long ns) override;
    void retract(RowData *input) override;
    void Cleanup(long ns) override;
    void close() override {}
};


#endif //COUNTWINDOWAGGFUNCTION_H
