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

#include "RocksDBMemoryOptions.h"
thread_local int64_t RocksDBMemoryOptions::calculatedCacheCapacity = 3200LL * 1024LL * 1024LL;
thread_local double RocksDBMemoryOptions::highPriorityPoolRatio = 0.1;
thread_local int64_t RocksDBMemoryOptions::writeBufferManagerCapacity = 4800LL * 1024LL * 1024LL;