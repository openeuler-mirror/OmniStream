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
#ifndef OMNISTREAM_CHANNEL_STATE_PENDING_RESULT_H
#define OMNISTREAM_CHANNEL_STATE_PENDING_RESULT_H

#include <map>
#include <exception>
#include "runtime/partition/consumer/InputChannelInfo.h"
#include "runtime/partition/ResultSubpartitionInfoPOD.h"
#include "runtime/state/AbstractChannelStateHandle.h"
#include "runtime/state/StreamStateHandle.h"
#include "ChannelStateSerializer.h"
#include "runtime/checkpoint/channel/ChannelStateWriter.h"

namespace omnistream {

    class ChannelStatePendingResult {
    public:
        ChannelStatePendingResult(
            int subtaskIndex,
            int64_t checkpointId,
            ChannelStateWriter::ChannelStateWriteResult &result,
            ChannelStateSerializer *serializer);

        bool IsAllInputsReceived() const;
        bool IsAllOutputsReceived() const;

        std::map<InputChannelInfo, AbstractChannelStateHandle<InputChannelInfo>::StateContentMetaInfo> &GetInputChannelOffsets();
        std::map<ResultSubpartitionInfoPOD, AbstractChannelStateHandle<ResultSubpartitionInfoPOD>::StateContentMetaInfo> &GetResultSubpartitionOffsets();

        void CompleteInput();
        void CompleteOutput();
        void FinishResult(StreamStateHandle *stateHandle);
        void Fail(const std::exception_ptr &e);
        bool IsDone() const;

    private:
        const int subtaskIndex;
        const int64_t checkpointId;
        ChannelStateWriter::ChannelStateWriteResult &result;
        ChannelStateSerializer *serializer;
        bool allInputsReceived;
        bool allOutputsReceived;

        std::map<InputChannelInfo, AbstractChannelStateHandle<InputChannelInfo>::StateContentMetaInfo> inputChannelOffsets;
        std::map<ResultSubpartitionInfoPOD, AbstractChannelStateHandle<ResultSubpartitionInfoPOD>::StateContentMetaInfo> resultSubpartitionOffsets;
    };

} // namespace omnistream

#endif // OMNISTREAM_CHANNEL_STATE_PENDING_RESULT_H
