/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_PULLINGASYNCDATAINPUT_H
#define OMNISTREAM_PULLINGASYNCDATAINPUT_H

#include <memory>
#include <optional>
#include <string>
#include "AvailabilityProvider.h"


namespace omnistream {

    template <typename T>
    class PullingAsyncDataInput : public AvailabilityProvider {
    public:
        ~PullingAsyncDataInput() override = default;

        virtual std::optional<std::shared_ptr<T>> pollNext() = 0;

        virtual bool isFinished() = 0;

        virtual bool hasReceivedEndOfData() = 0;

        virtual std::string toString() const
        {
            return "PullingAsyncDataInput";
        }
    };

} // namespace omnistream

#endif // OMNISTREAM_PULLINGASYNCDATAINPUT_H