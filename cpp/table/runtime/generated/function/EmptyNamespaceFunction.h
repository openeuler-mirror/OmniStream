//
// Created by c00572813 on 2025/3/7.
//

#ifndef OMNISTREAM_EMPTYFUNCTION_H
#define OMNISTREAM_EMPTYFUNCTION_H

#include <optional>
#include <stdexcept>
#include <cstdint>
#include "table/runtime/generated/AggsHandleFunction.h"
#include "table/data/GenericRowData.h"
#include "table/runtime/dataview/StateDataViewStore.h"

class EmptyNamespaceFunction : public AggsHandleFunction {
public:
    void getValue(BinaryRowData* aggValue) override {
        aggValue = new BinaryRowData(0);
    }
    void setWindowSize(int windowSize) override {}
    bool equaliser(BinaryRowData* r1, BinaryRowData* r2) override {
        return false;
    }
    void accumulate(RowData* input) override {}
    void accumulate(omnistream::VectorBatch *input, const std::vector<int>& indices) override {}

    void retract(RowData* input) override {
        throw std::runtime_error("This function does not require retract method, but the retract method is called.");
    }
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override
    {
        throw std::runtime_error("This function does not require retract method, but the retract method is called.");
    }
    void merge(RowData* accumulators) override {}
    void createAccumulators(BinaryRowData* accumulators) override {
        accumulators = new BinaryRowData(0);
    }
    void setAccumulators(RowData* accumulators) override {}
    void resetAccumulators() override {}
    void getAccumulators(BinaryRowData* accumulators) override {}
    void cleanup() override {}
    void close() override {}
};


#endif //OMNISTREAM_EMPTYFUNCTION_H
