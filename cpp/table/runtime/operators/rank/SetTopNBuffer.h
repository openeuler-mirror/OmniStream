#pragma once
#include "data/util/VectorBatchUtil.h"

#include <set>
#include <vector>

/**
 * In-memory ordered TopN buffer.
 * Stores comboIds (uint64_t), sorted by Comparator.
 * Uses multiset so equal-sort rows can coexist.
 */
template <typename Comparator>
class SetTopNBuffer {
public:
    using Buffer = std::multiset<omnistream::ComboId, Comparator>;

    explicit SetTopNBuffer(Comparator cmp) : buffer_(cmp), bufferId(-99)
    {
    }

    inline auto begin()
    {
        return buffer_.begin();
    }
    inline auto end()
    {
        return buffer_.end();
    }

    inline bool AddElement(omnistream::ComboId id)
    {
        buffer_.insert(id);
        return true;
    }

    inline void RemoveSmallestElement()
    {
        if (!buffer_.empty()) {
            auto it = buffer_.end();
            --it;
            buffer_.erase(it);
        }
    }

    inline int GetSize() const
    {
        return (int)buffer_.size();
    }

    inline omnistream::ComboId GetSmallestElement() const
    {
        auto it = buffer_.end();
        --it;
        return *it;
    }

    inline void LoadFromPlainVector(const std::vector<omnistream::ComboId>& plain)
    {
        buffer_.clear();
        buffer_.insert(plain.begin(), plain.end());
    }

    inline std::vector<omnistream::ComboId>* ToPlainVector() const
    {
        return new std::vector<omnistream::ComboId>(buffer_.begin(), buffer_.end());
    }
    inline void SetBufferId(int id)
    {
        bufferId = id;
    }
    inline int GetBufferId() const
    {
        return bufferId;
    }

private:
    Buffer buffer_;
    int bufferId; // unique buffer identifier
};
