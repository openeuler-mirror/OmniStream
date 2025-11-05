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
#pragma once
#include <memory>
#include <typeinfo>
#include "RuntimeEvent.h"

namespace omnistream {
    class EndOfPartitionEvent : public RuntimeEvent {
    public:
        static std::shared_ptr<EndOfPartitionEvent> getInstance()
        {
            static std::shared_ptr<EndOfPartitionEvent> instance =
                std::make_shared<EndOfPartitionEvent>();
            return instance;
        }
        int GetEventClassID() override
        {
            return AbstractEvent::endOfPartition;
        }

        static int hashCode()
        {
            return 1965146673;
        }

        std::string GetEventClassName() override
        {
            return "EndOfPartitionEvent";
        }

        static bool equals(const RuntimeEvent& other)
        {
            return typeid(other) == typeid(EndOfPartitionEvent);
        }
    };
} // namespace omnistream
