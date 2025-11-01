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
#ifndef OMNISTREAM_COMPLETED_CHECKPOINT_STORAGE_LOCATION_H
#define OMNISTREAM_COMPLETED_CHECKPOINT_STORAGE_LOCATION_H

#include "runtime/state/StreamStateHandle.h"

namespace omnistream {

    class CompletedCheckpointStorageLocation {
    public:
        virtual ~CompletedCheckpointStorageLocation() = default;
        
        virtual StreamStateHandle* getMetadataHandle() = 0;
        virtual std::string getExternalPointer() = 0;
        virtual void disposeStorageLocation() = 0;
    };
}

#endif // OMNISTREAM_COMPLETED_CHECKPOINT_STORAGE_LOCATION_H