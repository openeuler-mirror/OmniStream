#pragma once

#include "table/data/util/VectorBatchUtil.h"

template <typename K>
class AppendOnlyTopNFunction;

/**
 * Comparator used inside std::set<long>.
 * Calls back into AppendOnlyTopNFunction to compare rows by sort key.
 */
template <typename K>
struct TopNDataComparator {
    AppendOnlyTopNFunction<K>* fn = nullptr;

    TopNDataComparator() = default;
    explicit TopNDataComparator(AppendOnlyTopNFunction<K>* f) : fn(f) {}

    inline bool operator()(long a, long b) const {
        if (a == b) return false;

        int batchIdA = VectorBatchUtil::getBatchId(a);
        int rowIdA   = VectorBatchUtil::getRowId(a);

        int batchIdB = VectorBatchUtil::getBatchId(b);
        int rowIdB   = VectorBatchUtil::getRowId(b);

        auto* vbA = fn->GetVectorBatch(batchIdA);
        auto* vbB = fn->GetVectorBatch(batchIdB);

        if (vbA == nullptr || vbB == nullptr) {
            std::cout << "Error: VectorBatch is null in TopNDataComparator." << std::endl;
            return false;
        }

        return fn->CompareRowData(vbA, rowIdA, vbB, rowIdB);

    }
};
