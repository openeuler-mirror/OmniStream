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


#ifndef OMNISTREAM_DEFAULTCONFIGURABLEOPTIONSFACTORY_H
#define OMNISTREAM_DEFAULTCONFIGURABLEOPTIONSFACTORY_H
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "include/functions/Configuration.h"
#include "core/configuration/MemorySize.h"
#include "RocksDBConfigurableOptions.h"

class DefaultConfigurableOptionsFactory {
public:
    static void createColumnOptions(ROCKSDB_NAMESPACE::ColumnFamilyOptions &currentOptions);
    static void createDBOptions(ROCKSDB_NAMESPACE::DBOptions &currentOptions);
};


#endif // OMNISTREAM_DEFAULTCONFIGURABLEOPTIONSFACTORY_H
