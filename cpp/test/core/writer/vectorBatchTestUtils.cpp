#include "vectorBatchTestUtils.h"
#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include <string_view>
#include <iostream>
#include <vector>
#include <gtest/gtest.h>

namespace omnistream {

omniruntime::vec::VectorBatch* createVectorBatch(std::vector<omniruntime::type::DataTypeId> inputTypes, int rowCount, bool doSetValue) {
    int colCount = inputTypes.size();

    omniruntime::vec::VectorBatch* vectorBatch = new omniruntime::vec::VectorBatch(rowCount);
    vectorBatch->ResizeVectorCount(colCount);

    // Create vectors for each column
    for (int i = 0; i < colCount; ++i) {
        if (inputTypes[i] == omniruntime::type::OMNI_LONG) {
            vectorBatch->SetVector(i, omniruntime::vec::VectorHelper::CreateVector(omniruntime::vec::OMNI_FLAT, omniruntime::type::OMNI_LONG, rowCount));
        } else if (inputTypes[i] == omniruntime::type::OMNI_CHAR) {
            vectorBatch->SetVector(i, omniruntime::vec::VectorHelper::CreateStringVector(rowCount));
        } else {
            throw std::runtime_error("Unsupported type: " + inputTypes[i]);
        }
    }

    if (doSetValue) {
        // Set values for each row
        for (size_t rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            for (size_t colIndex = 0; colIndex < inputTypes.size(); ++colIndex) {
            omniruntime::type::DataTypeId type = inputTypes[colIndex];

            if (type == omniruntime::type::OMNI_LONG) {
                omniruntime::vec::VectorHelper::SetValue(vectorBatch->Get(colIndex), rowIndex, (void*)&rowIndex);
            } else if (type == omniruntime::type::OMNI_CHAR) {
                std::string str = "str" + std::to_string(rowIndex);
                    omniruntime::vec::VectorHelper::SetValue(vectorBatch->Get(colIndex), rowIndex, (void*)&str);
                }
            }
        }
    }

    return vectorBatch;
}

} // namespace omnistream