/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */


#include "CompositeWindowAggFunction.h"



void CompositeWindowAggFunction::open(StateDataViewStore* store) {
    for (auto& func : functions_) {
        func->open(store);
    }
}

void CompositeWindowAggFunction::accumulate(RowData* accInput) {
    for (auto& func : functions_) {
        func->accumulate(accInput);
    }
}

void CompositeWindowAggFunction::retract(RowData* retractInput) {
    for (auto& func : functions_) {
        func->retract(retractInput);
    }
}

void CompositeWindowAggFunction::merge(int64_t ns, RowData* otherAcc) {
    for (auto& func : functions_) {
        func->merge(ns, otherAcc);
    }
}

void CompositeWindowAggFunction::setAccumulators(int64_t ns, RowData* acc) {
    for (auto& func : functions_) {
        func->setAccumulators(ns, acc);
    }
  
}

RowData* CompositeWindowAggFunction::getAccumulators() {
    RowData* res = nullptr;
    for (const auto &function : functions_) {
        res = function->getAccumulators();
    }
    return res;
}

RowData* CompositeWindowAggFunction::createAccumulators(int accumulatorArity) {
    auto* newAcc = BinaryRowData::createBinaryRowDataWithMem(accumulatorArity);
    for (int i = 0; i < accumulatorArity; ++i) {
        newAcc->setNullAt(i); 
    }
 
    return newAcc;
}

RowData* CompositeWindowAggFunction::getValue(int64_t ns) {
    RowData* currentAcc = nullptr;
    for (auto& func : functions_) {
        currentAcc = func->getValue(ns); 
    }
    int64_t startTime = sliceAssigner->getWindowStart(ns);
    BinaryRowData *windowResult = BinaryRowData::createBinaryRowDataWithMem(2);;
    windowResult->setLong(0, startTime);
    windowResult->setLong(1, ns);
    auto tempValue = new JoinedRowData();
    tempValue->replace(currentAcc, windowResult);
    auto* value = BinaryRowDataSerializer::joinedRowToBinaryRow(tempValue, outputTypeIds_);
    return value;
}

void CompositeWindowAggFunction::Cleanup(int64_t ns) {
    for (auto& func : functions_) {
        func->Cleanup(ns);
    }
}

void CompositeWindowAggFunction::close() {
    for (auto& func : functions_) {
        func->close();
    }
}
 
bool CompositeWindowAggFunction::isWindowEmpty(){
    bool res = true;
    for (auto& func : functions_) {
        res = res && func->isWindowEmpty();
    }
    return res;
} 

