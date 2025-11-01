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

#ifndef GENERATEDCSVLOOKUPFUNCTION_H
#define GENERATEDCSVLOOKUPFUNCTION_H

#include "table/runtime/collector/TableFunctionCollector.h"
#include "table/sources/CsvTableSource.h"
#include "functions/FlatMapFunction.h"

template<typename K>
class GeneratedCsvLookupFunction : public FlatMapFunction<omnistream::VectorBatch> {
public:
    explicit GeneratedCsvLookupFunction(CsvLookupFunction<K>* lookupFunc_) : lookupFunction(lookupFunc_) {};

    ~GeneratedCsvLookupFunction() override = default;

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
