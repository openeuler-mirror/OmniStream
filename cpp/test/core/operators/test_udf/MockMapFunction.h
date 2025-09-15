#ifndef FLINK_TNEL_MOCKMAPFUNCTION_H
#define FLINK_TNEL_MOCKMAPFUNCTION_H
#include "functions/MapFunction.h"
#include "test/core/operators/test_utils/Mocks.h"

class MockMapFunction : public MapFunction<Object> {
public:
    Object* map(Object* in) override {
        auto mockIn = dynamic_cast<MockObject*>(in);
        if (mockIn) {
            return new MockObject(mockIn->getValue() + 1);  // 返回一个新的对象，值 + 1
        }
        return nullptr;
    }
};
#endif //FLINK_TNEL_MOCKMAPFUNCTION_H
