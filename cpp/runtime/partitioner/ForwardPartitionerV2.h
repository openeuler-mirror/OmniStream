/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/7/25.
//

#ifndef FORWARDPARTITIONERV2_H
#define FORWARDPARTITIONERV2_H

#include "StreamPartitionerV2.h"

namespace omnistream {
    template<typename T>
    class ForwardPartitionerV2 : public StreamPartitionerV2<T> {
    public:
        // result should be vector
        int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) override
        {
            return 0;
        }

        int selectRowChannel(RowData* record) override
        {
            return 0;
        }

        std::unique_ptr<StreamPartitionerV2<T>> copy() override
        {
            return std::make_unique<ForwardPartitionerV2<T>>(*this);
        }

        bool isPointwise() override
        {
            return true;
        }

        [[nodiscard]] std::string toString() const override
        {
            return "FORWARD";
        }

        std::unique_ptr<SubtaskStateMapper> getDownstreamSubtaskStateMapper() override
        {
            return std::make_unique<SubtaskStateMapper>(UnsupportedMapper());
        }

        std::unique_ptr<SubtaskStateMapper> getUpstreamSubtaskStateMapper() override
        {
            return std::make_unique<SubtaskStateMapper>(UnsupportedMapper());
        }
    };
}

#endif //FORWARDPARTITIONERV2_H
