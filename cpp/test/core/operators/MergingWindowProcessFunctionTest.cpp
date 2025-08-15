//
// Created by q00649235 on 2025/3/18.
//

#include "table/runtime/operators/window/internal/MergingWindowProcessFunction.h"
#include "table/runtime/operators/window/assigners/SessionWindowAssigner.h"
#include "table/runtime/generated/function/TimeWindowCountWindowAggFunction.h"
#include <gtest/gtest.h>

TEST(MergingWindowProcessFunction, InitTest)
{
    // auto assigner = new SessionWindowAssigner(10000, true);
    // auto windowSerializer = new TimeWindow::Serializer();
    // long allowedLateness = 0;
    // NamespaceAggsHandleFunctionBase<TimeWindow>* aggFunction = new TimeWindowCountWindowAggFunction(0, 0, 0);
    // MergingWindowProcessFunction<GenericRowData, TimeWindow> func(assigner, aggFunction, windowSerializer, allowedLateness);
    //
    // // open but actually do nothing here
    // func.open()
}


// prepareAggregateAccumulatorForEmit

// open

// assignStateNamespace

// assignActualWindows

// cleanWindowIfNeeded