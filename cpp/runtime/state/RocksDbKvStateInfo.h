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

#ifndef OMNISTREAM_ROCKSDBKVSTATEINFO_H
#define OMNISTREAM_ROCKSDBKVSTATEINFO_H

#include <utility>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "runtime/state/RegisteredStateMetaInfoBase.h"

class RocksDbKvStateInfo {
public:
    RocksDbKvStateInfo(
            rocksdb::ColumnFamilyHandle* columnFamilyHandle,
            std::shared_ptr<RegisteredStateMetaInfoBase> metaInfo
    ): columnFamilyHandle_(columnFamilyHandle),
       metaInfo_(std::move(metaInfo))
    {};

    RocksDbKvStateInfo(): columnFamilyHandle_(nullptr),
                          metaInfo_(nullptr) {};

    void setColumnFamilyHandle(rocksdb::ColumnFamilyHandle* columnFamilyHandle)
    {
        columnFamilyHandle_ = columnFamilyHandle;
    }

    rocksdb::ColumnFamilyHandle* columnFamilyHandle_;
    std::shared_ptr<RegisteredStateMetaInfoBase> metaInfo_;
};
#endif // OMNISTREAM_ROCKSDBKVSTATEINFO_H
