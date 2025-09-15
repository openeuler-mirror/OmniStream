/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef SIMPLEPARTITIONER_H
#define SIMPLEPARTITIONER_H
#include "StreamPartitionerV2.h"


namespace omnistream {
    template<typename T>
    class SimplePartitioner : public StreamPartitionerV2<T> {
    public:
        SimplePartitioner() = default;
        void setup(int numberOfChannels) override
        {
            this->numberOfChannels = numberOfChannels;
        }

        int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) override
        {
            return 0;
        };

        int selectRowChannel(RowData* record) override
        {
            return 0;
        };

        std::unique_ptr<SubtaskStateMapper> getDownstreamSubtaskStateMapper() override
        {
            return std::make_unique<SubtaskStateMapper>(UnsupportedMapper());
        }

        std::unique_ptr<SubtaskStateMapper> getUpstreamSubtaskStateMapper() override
        {
            return std::make_unique<SubtaskStateMapper>(UnsupportedMapper());
        }

        bool isPointwise() override
        {
            return true;
        }

        std::unique_ptr<StreamPartitionerV2<T>> copy() override
        {
            return std::make_unique<SimplePartitioner<T>>(*this);
        }

        [[nodiscard]] std::string toString() const override
        {
            return "SIMPLE";
        }
    };

}


#endif //SIMPLEPARTITIONER_H
