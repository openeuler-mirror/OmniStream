#include "MockKeyedBy.h"

extern "C" std::unique_ptr<KeySelect<Object>> NewInstance() {
    return std::make_unique<MockKeyedBy>();
}