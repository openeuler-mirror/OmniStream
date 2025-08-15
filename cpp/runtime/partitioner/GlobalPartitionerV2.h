/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef GLOBALPARTITIONERV2_H
#define GLOBALPARTITIONERV2_H
#include "StreamPartitionerV2.h"

namespace omnistream {
    template<typename T>
    class GlobalPartitionerV2 : public StreamPartitionerV2<T> {
    public:
        GlobalPartitionerV2() {}

        int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) override
        {
            return 0;
        }

        int selectRowChannel(RowData* record) override
        {
            return 0;
        }

        std::unique_ptr<SubtaskStateMapper> getDownstreamSubtaskStateMapper() override
        {
            return std::make_unique<SubtaskStateMapper>(UnsupportedMapper());
        }

        std::unique_ptr<SubtaskStateMapper> getUpstreamSubtaskStateMapper() override
        {
            return std::make_unique<SubtaskStateMapper>(UnsupportedMapper());
        }

        std::unique_ptr<StreamPartitionerV2<T>> copy() override
        {
            return std::make_unique<GlobalPartitionerV2<T>>(*this);
        }

        bool isPointwise() override
        {
            return false;
        }

        std::string toString() const override
        {
            return "GLOBAL";
        }
    };

}

#endif //GLOBALPARTITIONERV2_H
