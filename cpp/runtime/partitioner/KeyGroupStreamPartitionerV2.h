/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef KEYGROUPSTREAMPARTITIONERV2_H
#define KEYGROUPSTREAMPARTITIONERV2_H


#include <stdexcept>
#include <memory>
#include <typeinfo>
#include <cassert>
#include "KeySelector.h"
#include "StreamPartitionerV2.h"
#include "plugable/SerializationDelegate.h"
#include "state/KeyGroupRangeAssignment.h"

namespace omnistream {
    template<typename T, typename K>
    class KeyGroupStreamPartitionerV2 : public StreamPartitionerV2<T> {
    public:
        KeyGroupStreamPartitionerV2(KeySelector<K>* keySelector, int maxParallelism)
            : keySelector(keySelector), maxParallelism(maxParallelism)
        {
            if (maxParallelism <= 0) {
                throw std::invalid_argument("Number of key-groups must be > 0!");
            }
            if (!keySelector) {
                throw std::invalid_argument("Key selector cannot be null");
            }
        }

        int getMaxParallelism() const
        {
            return maxParallelism;
        }

        int selectRowChannel(omnistream::VectorBatch* record, int rowIndex) override
        {
            K key;
            bool enableKeyReuse = true;
            try {
                key = keySelector->getKey(record, rowIndex, enableKeyReuse);
            } catch (const std::exception& e) {
                throw std::runtime_error("Could not extract key from ");
            }
            auto assignment =
                KeyGroupRangeAssignment<K>::assignKeyToParallelOperator(key, maxParallelism, this->numberOfChannels);
            // Only delete if memory reuse is NOT enabled or is enabled but NOT possible
            if constexpr (std::is_pointer_v<K>) {
                if (!enableKeyReuse || (enableKeyReuse && !(keySelector->canReuseKey()))) {
                    delete key;
                }
            }
            return assignment;
        }

        int selectRowChannel(RowData* record) override
        {
            K key;
            try {
                key = keySelector->getKey(record);
            } catch (const std::exception& e) {
                throw std::runtime_error("Could not extract key from ");
            }
            return KeyGroupRangeAssignment<K>::assignKeyToParallelOperator(key, maxParallelism, this->numberOfChannels);
        }

        std::unique_ptr<SubtaskStateMapper> getDownstreamSubtaskStateMapper() override
        {
            return std::make_unique<SubtaskStateMapper>(RoundRobinMapper());
        }

        std::unique_ptr<SubtaskStateMapper> getUpstreamSubtaskStateMapper() override
        {
            return std::make_unique<SubtaskStateMapper>(ArbitraryMapper());
        }

        std::unique_ptr<StreamPartitionerV2<T>> copy() override
        {
            return std::make_unique<KeyGroupStreamPartitionerV2<T, K>>(*this);
        }

        bool isPointwise() override
        {
            return false;
        }

        [[nodiscard]] std::string toString() const override
        {
            return "HASH";
        }

        void configure(int newMaxParallelism)
        {
            this->maxParallelism = newMaxParallelism;
        }
    private:
        KeySelector<K>* keySelector;
        int maxParallelism;
    };
}

#endif //KEYGROUPSTREAMPARTITIONERV2_H
