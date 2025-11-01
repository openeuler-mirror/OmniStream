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

#ifndef FLINK_TNEL_STREAMTASKNETWORKINPUT_H
#define FLINK_TNEL_STREAMTASKNETWORKINPUT_H

#include <unordered_map>
#include "runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.h"
#include "AbstractStreamTaskNetworkInput.h"

namespace omnistream::datastream {
    class StreamTaskNetworkInput : public AbstractStreamTaskNetworkInput {
    public:
        explicit StreamTaskNetworkInput(TypeSerializer* inputSerializer, std::vector<long>& channelInfos);
    private:
        static std::unique_ptr<std::unordered_map<long, RecordDeserializer *>>
        getRecordDeserializers(std::vector<long>& channelInfos);
    };
}


#endif // FLINK_TNEL_STREAMTASKNETWORKINPUT_H
