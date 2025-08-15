//
// Created by root on 4/12/25.
//

#ifndef FLINK_TNEL_MOCKREDUCEFUNCTION_H
#define FLINK_TNEL_MOCKREDUCEFUNCTION_H
#include "functions/ReduceFunction.h"
#include "basictypes/Long.h"
#include "basictypes/Tuple2.h"

class MockReduceFunction : public ReduceFunction<Object> {
public:
    Object* reduce(Object* in1, Object* in2) override {
        // 假设 in1 和 in2 都是 Tuple2，且 f1 是 Long
        auto tuple1 = dynamic_cast<Tuple2*>(in1);
        auto tuple2 = dynamic_cast<Tuple2*>(in2);
        if (tuple1 && tuple2) {
            auto long1 = dynamic_cast<Long*>(tuple1->f1);
            auto long2 = dynamic_cast<Long*>(tuple2->f1);
            if (long1 && long2) {
                long1->setValue(long1->getValue() + long2->getValue());
                return tuple1->clone();
            }
        }
        return nullptr;
    }
};
#endif //FLINK_TNEL_MOCKREDUCEFUNCTION_H
