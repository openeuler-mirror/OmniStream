/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by xichen on 1/30/25.
//

#ifndef GENERATEDCSVLOOKUPFUNCTION_H
#define GENERATEDCSVLOOKUPFUNCTION_H

#include "table/runtime/collector/TableFunctionCollector.h"
#include "table/sources/CsvTableSource.h"
#include "functions/FlatMapFunction.h"

template<typename K>
class GeneratedCsvLookupFunction : public FlatMapFunction<omnistream::VectorBatch> {
public:
    GeneratedCsvLookupFunction(CsvLookupFunction<K>* lookupFunc_) : lookupFunction(lookupFunc_) {};

    void open()
    {
        // the hash table is built here
        lookupFunction->open();
    }
    void flatMap(omnistream::VectorBatch* in, Collector* collector) override
    {
        lookupFunction->eval(in, collector);
    }
private:
    CsvLookupFunction<K>* lookupFunction;
    // RowRowConverter converter;
};
#endif
