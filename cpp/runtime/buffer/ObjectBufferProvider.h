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

#ifndef OMNISTREAM_OBJECTBUFFERPROVIDER_H
#define OMNISTREAM_OBJECTBUFFERPROVIDER_H

#include <memory>
#include <string>
#include <io/AvailabilityProvider.h>


#include "ObjectBuffer.h"
#include "ObjectBufferBuilder.h"
#include "ObjectBufferListener.h"
#include "BufferProvider.h"

namespace omnistream {

    class ObjectBufferProvider : public BufferProvider {
    public:
        ~ObjectBufferProvider() override = default;

        virtual bool addBufferListener(std::shared_ptr<ObjectBufferListener> listener) = 0;
    };

} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFERPROVIDER_H
