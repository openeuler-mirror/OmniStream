//
// Created by q00649235 on 2025/3/18.
//

#ifndef OMNISTREAM_TIMEWINDOWCOUNTWINDOWAGGFUNCTION_H
#define OMNISTREAM_TIMEWINDOWCOUNTWINDOWAGGFUNCTION_H

#include <stdexcept>
#include <optional>
#include "table/data/GenericRowData.h"
#include "table/runtime/dataview/StateDataViewStore.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "table/data/TimestampData.h"
#include "table/runtime/operators/window/TimeWindow.h"

// not used
class TimeWindowCountWindowAggFunction : public NamespaceAggsHandleFunction<TimeWindow> {
private:
    StateDataViewStore* store;
    TimeWindow namespaceVal;
    bool valueIsNull = false;
    int aggIdx;
    int accIndex;
    int valueIndex;
    long limit;
    long aggValue;

public:
    TimeWindowCountWindowAggFunction(int aggIdx, int accIndex, int valueIndex)
            :aggIdx(aggIdx),
             accIndex(accIndex),
             valueIndex(valueIndex) {};
    void open(StateDataViewStore* store) override;
    void accumulate(RowData *accInput) override;
    void merge(TimeWindow ns, RowData *otherAcc) override;
    void setAccumulators(TimeWindow ns, RowData *acc) override;
    RowData *getAccumulators() override;
    RowData *createAccumulators(int accumulatorArity) override;
    RowData *getValue(TimeWindow ns) override;
    void retract(RowData *input) override;
    void Cleanup(TimeWindow ns) override;
    void close() override {}
};


#endif //OMNISTREAM_TIMEWINDOWCOUNTWINDOWAGGFUNCTION_H
