#ifndef OMNISTREAM_VECTOR_BATCH_TEST_UTILS_H
#define OMNISTREAM_VECTOR_BATCH_TEST_UTILS_H

#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "OmniOperatorJIT/core/src/type/data_type.h"
#include <string_view>
#include <iostream>
#include <vector>
#include <gtest/gtest.h>

namespace omnistream {

template <omniruntime::type::DataTypeId typeId>
static auto VectorGetValue(omniruntime::vec::BaseVector *vector, int32_t index) -> typename omniruntime::type::NativeType<typeId>::type {
    using T = typename omniruntime::type::NativeType<typeId>::type;
    if constexpr (std::is_same_v<T, std::string_view>) {
        return static_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>> *>(vector)->GetValue(index);
    } else {
        return static_cast<omniruntime::vec::Vector<T> *>(vector)->GetValue(index);
    }
}

omniruntime::vec::VectorBatch *createVectorBatch(std::vector<omniruntime::type::DataTypeId> inputTypes, int rowCount, bool doSetValue = false);

} // namespace omnistream

#endif // OMNISTREAM_VECTOR_BATCH_TEST_UTILS_H