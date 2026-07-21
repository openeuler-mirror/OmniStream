/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "OperatorSavepointAdaptorFactory.h"
#include "OperatorSavepointAdaptor.h"
#include "DeduplicateSavepointAdaptor.h"

namespace omnistream {
std::unique_ptr<OperatorSavepointAdaptor> OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType type)
{
    switch (type) {
        case FlinkSavepointAdaptorType::DeduplicateAdaptor: return std::make_unique<DeduplicateSavepointAdaptor>();
        case FlinkSavepointAdaptorType::AppendOnlyTopNAdaptor:
        case FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor:
        case FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor:
            INFO_RELEASE(
                "OperatorSavepointAdaptorFactory::createAdaptor - adaptor type " << static_cast<int>(type)
                                                                                 << " not yet implemented");
            return nullptr;
        case FlinkSavepointAdaptorType::OmniIsCompatible:
        case FlinkSavepointAdaptorType::None:
        default: return nullptr;
    }
}
} // namespace omnistream
