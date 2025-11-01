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

#include "StreamTaskNetworkInputFactory.h"
namespace omnistream::datastream {
    std::unique_ptr<StreamTaskNetworkInput> StreamTaskNetworkInputFactory::create(TypeSerializer *inputSerializer,
                                                                                  std::vector<long> & channelInfos)
    {
        LOG(">>>>>>>>")

        return std::make_unique<StreamTaskNetworkInput>(inputSerializer, channelInfos);
    }
}
