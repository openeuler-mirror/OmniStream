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

#ifndef OMNISTREAM_INPUTCHANNELSTATEHANDLE_H
#define OMNISTREAM_INPUTCHANNELSTATEHANDLE_H
#include "runtime/state/StateObject.h"
class InputChannelStateHandle : public StateObject {
public:
    // This class should be a template with InputChannelInfo, and info should be of class InputChannelInfo
    // This is a temporary place holder for checkpointing's PioritizedOperatorSubtaskState
    std::string getInfo() const
    {
        return info;
    }
    void DiscardState() override {}

    long GetStateSize() const override
    {
        return 0;
    };

    std::string ToString() const override
    {
        return "InputChannelStateHandle";
    };
private:
    std::string info = "InputChannelStateHandle";
};
#endif // OMNISTREAM_INPUTCHANNELSTATEHANDLE_H
