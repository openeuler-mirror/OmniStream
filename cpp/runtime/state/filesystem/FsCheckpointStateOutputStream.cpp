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
#include "FsCheckpointStateOutputStream.h"
#include <fstream>

FsCheckpointStateOutputStream::FsCheckpointStateOutputStream(
    Path basePath, int fs,
    int bufferSize, int localStateThreshold, bool allowRelativePaths)
    : basePath_(std::move(basePath)),
      fs_(fs),
      bufferSize_(bufferSize),
      localStateThreshold_(localStateThreshold),
      allowRelativePaths_(allowRelativePaths),
      closed_(false) {
    tempPath_ = Path(basePath_, relativeStatePath_ + ".tmp").toString(); // relativeStatePath_ is UUID
    finalPath_ = Path(basePath_, relativeStatePath_).toString();

    outStream_ = new std::ofstream(tempPath_, std::ios::binary);
    if (!static_cast<std::ofstream*>(outStream_)->is_open()) {
        throw std::runtime_error("Failed to open temp file for checkpoint output: " + tempPath_);
    }
}

void FsCheckpointStateOutputStream::Write(const void* data, size_t length)
{
    static_cast<std::ofstream*>(outStream_)->write(reinterpret_cast<const char*>(data), length);
}

void FsCheckpointStateOutputStream::Flush()
{
    static_cast<std::ofstream*>(outStream_)->flush();
}

long FsCheckpointStateOutputStream::GetPos()
{
    if (!outStream_) {
        throw std::runtime_error("Stream not open");
    }
    auto* fbuf = static_cast<std::ofstream*>(outStream_)->rdbuf();
    std::streampos pos = fbuf->pubseekoff(0, std::ios::cur, std::ios::out);
    if (pos < 0) {
        throw std::runtime_error("Failed to get current position");
    }
    return static_cast<long>(pos);
}

void FsCheckpointStateOutputStream::Sync()
{
    NOT_IMPL_EXCEPTION
}

void FsCheckpointStateOutputStream::Close()
{
    NOT_IMPL_EXCEPTION
}

StreamStateHandle* FsCheckpointStateOutputStream::CloseAndGetHandle()
{
    NOT_IMPL_EXCEPTION
}

bool FsCheckpointStateOutputStream::IsClosed()
{
    return closed_;
}
