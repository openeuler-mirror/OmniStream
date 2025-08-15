#include "MockMapFunction.h"

extern "C" std::unique_ptr<MapFunction<Object>> NewInstance() {
    return std::make_unique<MockMapFunction>();
}