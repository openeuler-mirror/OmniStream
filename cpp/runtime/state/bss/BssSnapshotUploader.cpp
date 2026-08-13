/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "BssSnapshotUploader.h"

#ifdef WITH_OMNISTATESTORE

#include <algorithm>

#include "runtime/state/rocksdb/RocksDBStateUploader.h"

namespace bss_adapter {

std::vector<IncrementalRemoteKeyedStateHandle::HandleAndLocalPath> UploadSnapshotFiles(
    const std::shared_ptr<omnistream::OmniTaskBridge>& bridge,
    const std::vector<std::filesystem::path>& files,
    int numberOfTransferThreads)
{
    RocksDBStateUploader uploader(std::max(1, numberOfTransferThreads));
    return uploader.callUploadFilesToCheckpointFs(bridge, files);
}

} // namespace bss_adapter

#endif // WITH_OMNISTATESTORE
