#include "MockReduceFunction.h"
#include <memory>

extern "C" std::unique_ptr<ReduceFunction<Object>> NewInstance() {
    return std::make_unique<MockReduceFunction>();
}