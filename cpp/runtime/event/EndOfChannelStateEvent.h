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
#include <typeinfo>
#include "RuntimeEvent.h"

namespace omnistream {
    class EndOfChannelStateEvent : public RuntimeEvent {
    public:
        static EndOfChannelStateEvent &getInstance();
        // {
        //     static EndOfChannelStateEvent instance;
        // }

        static int hashCode()
        {
            return 1965146670;
        }

        static bool equals(RuntimeEvent &other)
        {
            return typeid(other) == typeid(EndOfChannelStateEvent);
        }

        std::string GetEventClassName() override
        {
            return "EndOfChannelStateEvent";
        }

    private:
        EndOfChannelStateEvent() {}
    };
} // namespace omnistream
