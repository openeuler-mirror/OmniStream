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

#ifndef OMNISTREAM_ROCKSDBCONFIGURABLEOPTIONS_H
#define OMNISTREAM_ROCKSDBCONFIGURABLEOPTIONS_H
#include "core/include/common.h"
#include "core/configuration/ConfigOption.h"
#include "basictypes/Integer.h"
#include "basictypes/String.h"
#include "basictypes/Double.h"
#include "basictypes/java_lang_Boolean.h"
#include "rocksdb/InfoLogLevel.h"
#include "rocksdb/CompactionStyle.h"

class RocksDBConfigurableOptions {
public:
    static std::unique_ptr<ConfigOption> MAX_BACKGROUND_THREADS;
    static std::unique_ptr<ConfigOption> MAX_OPEN_FILES;
    static std::unique_ptr<ConfigOption> LOG_MAX_FILE_SIZE;
    static std::unique_ptr<ConfigOption> LOG_FILE_NUM;
    static std::unique_ptr<ConfigOption> LOG_DIR;
    static std::unique_ptr<ConfigOption> LOG_LEVEL;
    static std::unique_ptr<ConfigOption> COMPACTION_STYLE;
    static std::unique_ptr<ConfigOption> USE_DYNAMIC_LEVEL_SIZE;
    static std::unique_ptr<ConfigOption> TARGET_FILE_SIZE_BASE;
    static std::unique_ptr<ConfigOption> MAX_SIZE_LEVEL_BASE;
    static std::unique_ptr<ConfigOption> WRITE_BUFFER_SIZE;
    static std::unique_ptr<ConfigOption> MAX_WRITE_BUFFER_NUMBER;
    static std::unique_ptr<ConfigOption> MIN_WRITE_BUFFER_NUMBER_TO_MERGE;
    static std::unique_ptr<ConfigOption> BLOCK_SIZE;
    static std::unique_ptr<ConfigOption> METADATA_BLOCK_SIZE;
    static std::unique_ptr<ConfigOption> BLOCK_CACHE_SIZE;
    static std::unique_ptr<ConfigOption> WRITE_BATCH_SIZE;
    static std::unique_ptr<ConfigOption> USE_BLOOM_FILTER;
    static std::unique_ptr<ConfigOption> BLOOM_FILTER_BITS_PER_KEY;
    static std::unique_ptr<ConfigOption> BLOOM_FILTER_BLOCK_BASED_MODE;
    static std::unique_ptr<ConfigOption> RESTORE_OVERLAP_FRACTION_THRESHOLD;
};


#endif // OMNISTREAM_ROCKSDBCONFIGURABLEOPTIONS_H
