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
#ifndef OMNISTREAM_LOCALRECOVERYDIRECTORYPROVIDER_H
#define OMNISTREAM_LOCALRECOVERYDIRECTORYPROVIDER_H

#include <string>
#include <filesystem>

class LocalRecoveryDirectoryProvider {
public:
    virtual ~LocalRecoveryDirectoryProvider() = default;

    virtual std::filesystem::path AllocationBaseDirectory(long checkpointId) = 0;
    virtual std::filesystem::path SubtaskBaseDirectory(long checkpointId) = 0;
    virtual std::filesystem::path SubtaskSpecificCheckpointDirectory(long checkpointId) = 0;
    
    virtual std::filesystem::path SelectAllocationBaseDirectory(int idx) = 0;
    virtual std::filesystem::path SelectSubtaskBaseDirectory(int idx) = 0;
    
    virtual int AllocationBaseDirsCount() const = 0;
    
    virtual std::string ToString() const = 0;
};

#endif // OMNISTREAM_LOCALRECOVERYDIRECTORYPROVIDER_H